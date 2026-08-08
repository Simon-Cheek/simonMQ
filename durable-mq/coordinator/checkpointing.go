package coordinator

import (
	"fmt"
	"log"

	"durable-mq/model"
	"durable-mq/record"

	"github.com/google/uuid"
)

func (c *Coordinator) maybeCheckpoint() {
	if !c.log.ShouldCheckpoint() {
		return
	}
	if !c.checkpointing.CompareAndSwap(false, true) {
		return
	}
	go func() {
		defer c.checkpointing.Store(false)
		// A checkpoint is an optimization, never a correctness requirement:
		// the WAL alone always replays to correct state. Nothing that happens
		// in here may take down the serving path, so failures — including an
		// unexpected panic — are logged and left for a later write to retry.
		defer func() {
			if r := recover(); r != nil {
				log.Printf("durable-mq: checkpoint panicked, retrying on a later write: %v", r)
			}
		}()
		if err := c.runCheckpoint(); err != nil {
			log.Printf("durable-mq: checkpoint failed, retrying on a later write: %v", err)
		}
	}()
}

func (c *Coordinator) appendLocked(rec *record.Record) (uint64, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.log.Append(rec)
}

// runCheckpoint derives and installs one checkpoint. Bailing out at any step
// is safe: without a durable END_CHECKPOINT nothing treats the work as valid,
// so a half-finished attempt leaves only files that startup cleanup sweeps —
// an orphan .tmp, an unreferenced .ckpt, or a BEGIN_CHECKPOINT replay ignores.
func (c *Coordinator) runCheckpoint() error {
	// Append BEGIN_CHECKPOINT
	rec := record.Record{
		OpType: record.OpBeginCheckpoint,
	}
	lsn, err := c.appendLocked(&rec)
	if err != nil {
		return fmt.Errorf("appending BEGIN_CHECKPOINT: %w", err)
	}

	// Fetch all logs up until BEGIN_CHECKPOINT
	fetchedRecs, err := c.log.ReadUpTo(lsn)
	if err != nil {
		return fmt.Errorf("reading the log up to LSN %d: %w", lsn, err)
	}

	// Compile new list of records using dedup logic
	compactedRecs := CompactRecords(fetchedRecs)

	// Write checkpoint file to disk
	checkpointFileId, err := uuid.NewUUID()
	if err != nil {
		return fmt.Errorf("generating a checkpoint file id: %w", err)
	}
	checkpointFileName, checksum, err := c.log.WriteCheckpointFile(checkpointFileId.String(), compactedRecs)
	if err != nil {
		return fmt.Errorf("writing the checkpoint file: %w", err)
	}

	// Write END_CHECKPOINT log
	endCkpt := model.EndCheckpoint{
		FileName:     checkpointFileName,
		FileChecksum: checksum,
	}
	payload, err := model.EncodeEndCheckpoint(endCkpt)
	if err != nil {
		return fmt.Errorf("encoding the END_CHECKPOINT payload: %w", err)
	}
	endCkptRec := record.Record{
		OpType:  record.OpEndCheckpoint,
		Payload: payload,
	}
	if _, err := c.appendLocked(&endCkptRec); err != nil {
		return fmt.Errorf("appending END_CHECKPOINT for %s: %w", checkpointFileName, err)
	}

	// Delete old, unnecessary files. Best-effort past this point — the
	// checkpoint is already durable, so leftovers cost disk, not correctness.
	if err := c.log.DeleteSegmentsBefore(lsn); err != nil {
		log.Printf("durable-mq: cleaning up segments before LSN %d: %v", lsn, err)
	}
	if err := c.log.DeleteCheckpointFilesExcept(checkpointFileName); err != nil {
		log.Printf("durable-mq: cleaning up superseded checkpoint files: %v", err)
	}
	return nil
}

type compactionState struct {
	createdQueues    map[string]*record.Record            // queueName -> record
	createdSubs      map[string]map[string]*record.Record // queueName -> subName -> record
	inFlightMessages map[string]*record.Record            // msgId -> Enqueue record
	remainingSubs    map[string]map[string]struct{}       // msgId -> set of subs (removed when acked)
	inFlightAcks     map[string]map[string]*record.Record // msgId -> subName -> Ack record
}

func newCompactionState() *compactionState {
	return &compactionState{
		createdQueues:    make(map[string]*record.Record),
		createdSubs:      make(map[string]map[string]*record.Record),
		inFlightMessages: make(map[string]*record.Record),
		remainingSubs:    make(map[string]map[string]struct{}),
		inFlightAcks:     make(map[string]map[string]*record.Record),
	}
}

// CompactRecords collapses a window of WAL records into the smallest set that
// replays to the same state. Pure — it reads nothing but recs.
func CompactRecords(recs []*record.Record) []*record.Record {
	s := newCompactionState()

	for _, rec := range recs {
		switch rec.OpType {
		case record.OpEnqueue:
			s.applyEnqueue(rec)
		case record.OpAck:
			s.applyAck(rec)
		case record.OpBeginCheckpoint, record.OpEndCheckpoint:
			continue
		case record.OpCreateQueue:
			s.applyCreateQueue(rec)
		case record.OpDeleteQueue:
			s.applyDeleteQueue(rec)
		case record.OpUpdateSubPolicy:
			s.applyUpdateSubPolicy(rec)
		case record.OpDeleteSubPolicy:
			s.applyDeleteSubPolicy(rec)
		}
	}

	return s.compacted()
}

func (s *compactionState) applyDeleteQueue(rec *record.Record) {
	qName := rec.QueueName
	delete(s.createdQueues, qName)
	delete(s.createdSubs, qName)

	for msgId, enqRec := range s.inFlightMessages {
		if enqRec.QueueName != qName {
			continue
		}
		delete(s.inFlightMessages, msgId)
		delete(s.remainingSubs, msgId)
		delete(s.inFlightAcks, msgId)
	}
}

func (s *compactionState) applyEnqueue(rec *record.Record) {
	qName := rec.QueueName
	if _, ok := s.createdQueues[qName]; !ok {
		return
	}
	enq, err := model.DecodeEnqueue(rec.Payload)
	if err != nil {
		return
	}
	if len(enq.SubList) == 0 {
		return
	} // Enqueued with nobody subscribed: just drop
	s.inFlightMessages[enq.MsgId] = rec
	s.remainingSubs[enq.MsgId] = make(map[string]struct{})
	for _, sub := range enq.SubList {
		s.remainingSubs[enq.MsgId][sub.SubName] = struct{}{}
	}
}

func (s *compactionState) applyAck(rec *record.Record) {
	ack, err := model.DecodeAck(rec.Payload)
	if err != nil {
		return
	}
	delete(s.remainingSubs[ack.MsgId], ack.SubName)

	if _, ok := s.inFlightAcks[ack.MsgId]; !ok {
		s.inFlightAcks[ack.MsgId] = make(map[string]*record.Record)
	}
	s.inFlightAcks[ack.MsgId][ack.SubName] = rec

	if len(s.remainingSubs[ack.MsgId]) == 0 {
		delete(s.remainingSubs, ack.MsgId)
		delete(s.inFlightMessages, ack.MsgId)
		delete(s.inFlightAcks, ack.MsgId)
	}
}

func (s *compactionState) applyCreateQueue(rec *record.Record) {
	qName := rec.QueueName
	s.createdQueues[qName] = rec
	// Unconditional reset, mirroring catalog.createQueue: re-creating a queue
	// under an existing name discards its policies rather than merging them.
	s.createdSubs[qName] = make(map[string]*record.Record)
}

func (s *compactionState) applyUpdateSubPolicy(rec *record.Record) {
	qName := rec.QueueName
	subP, err := model.DecodeSubPolicy(rec.Payload)
	if err != nil {
		return
	}
	if _, ok := s.createdSubs[qName]; !ok {
		return
	}
	s.createdSubs[qName][subP.SubName] = rec
}

func (s *compactionState) applyDeleteSubPolicy(rec *record.Record) {
	qName := rec.QueueName
	subP, err := model.DecodeSubPolicy(rec.Payload)
	if err != nil {
		return
	}
	if _, ok := s.createdSubs[qName]; !ok {
		return
	}
	delete(s.createdSubs[qName], subP.SubName)
}

func (s *compactionState) compacted() []*record.Record {
	compactedRecs := make([]*record.Record, 0, len(s.createdQueues)+len(s.inFlightMessages))

	for _, rec := range s.createdQueues {
		compactedRecs = append(compactedRecs, rec)
	}
	for _, subs := range s.createdSubs {
		for _, rec := range subs {
			compactedRecs = append(compactedRecs, rec)
		}
	}
	for _, rec := range s.inFlightMessages {
		compactedRecs = append(compactedRecs, rec)
	}
	for _, acks := range s.inFlightAcks {
		for _, rec := range acks {
			compactedRecs = append(compactedRecs, rec)
		}
	}

	return compactedRecs
}
