package coordinator

import (
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
		c.runCheckpoint()
	}()
}

func (c *Coordinator) appendLocked(rec *record.Record) (uint64, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.log.Append(rec)
}

func (c *Coordinator) runCheckpoint() {
	// Append BEGIN_CHECKPOINT
	rec := record.Record{
		OpType: record.OpBeginCheckpoint,
	}
	lsn, err := c.appendLocked(&rec)
	if err != nil {
		panic(err)
	}

	// Fetch all logs up until BEGIN_CHECKPOINT
	fetchedRecs, err := c.log.ReadUpTo(lsn)
	if err != nil {
		panic(err)
	}

	// Compile new list of records using dedup logic
	compactedRecs := c.compactRecs(fetchedRecs)

	// Write checkpoint file to disk
	checkpointFileId, err := uuid.NewUUID()
	if err != nil {
		panic(err)
	}
	checkpointFileName, checksum, err := c.log.WriteCheckpointFile(checkpointFileId.String(), compactedRecs)
	if err != nil {
		panic(err)
	}

	// Write END_CHECKPOINT log
	endCkpt := model.EndCheckpoint{
		FileName:     checkpointFileName,
		FileChecksum: checksum,
	}
	payload, err := model.EncodeEndCheckpoint(endCkpt)
	if err != nil {
		panic(err)
	}
	endCkptRec := record.Record{
		OpType:  record.OpEndCheckpoint,
		Payload: payload,
	}
	if _, err := c.appendLocked(&endCkptRec); err != nil {
		panic(err)
	}

	// Delete old, unnecessary files
	c.log.DeleteSegmentsBefore(lsn)
	c.log.DeleteCheckpointFilesExcept(checkpointFileName)
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

func (c *Coordinator) compactRecs(recs []*record.Record) []*record.Record {
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
			qName := rec.QueueName
			delete(s.createdQueues, qName)
			delete(s.createdSubs, qName)
		case record.OpUpdateSubPolicy:
			s.applyUpdateSubPolicy(rec)
		case record.OpDeleteSubPolicy:
			s.applyDeleteSubPolicy(rec)
		}
	}

	return s.compacted()
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
	if _, ok := s.createdQueues[qName]; !ok {
		s.createdQueues[qName] = rec
	}
	if _, ok := s.createdSubs[qName]; !ok {
		s.createdSubs[qName] = make(map[string]*record.Record)
	}
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
