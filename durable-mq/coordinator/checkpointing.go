package coordinator

import (
	"durable-mq/model"
	"durable-mq/record"
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

func (c *Coordinator) runCheckpoint() {
	// Append BEGIN_CHECKPOINT
	rec := record.Record{
		OpType: record.OpBeginCheckpoint,
	}
	lsn, err := c.log.Append(&rec)
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

	// Write END_CHECKPOINT log

	// Delete old, unnecessary files
}

func (c *Coordinator) compactRecs(recs []*record.Record) []*record.Record {
	var createdQueues []record.Record
	var createdSubs []record.Record
	inFlightMessages := make(map[string]*record.Record)   // Maps msgId to Enqueue Records
	remainingSubs := make(map[string]map[string]struct{}) // Maps msgId to Set of Subs (Remove when acked)

	for _, rec := range recs {

	}
}
