package coordinator

import (
	"durable-mq/catalog"
	"durable-mq/delivery"
	"durable-mq/model"
	"durable-mq/record"
	"durable-mq/wal"
	"fmt"
	"sync"
	"sync/atomic"
)

// Coordinator manages the initial replay of the WAL
// Acts as a wrapper of WAL for writes during queue runtime
// Acts as a wrapper of Catalog as well, coordinating Queue and Sub Metadata
type Coordinator struct {
	log  *wal.WAL
	cat  *catalog.Catalog
	deli *delivery.Delivery
	mu   sync.Mutex // Prevent Append operations from happening while replay occurs

	checkpointing atomic.Bool // true for the entire lifecycle of an in-flight checkpoint, cleanup included
}

// WAL Config
const walDirectory = "durable-wal"
const maxLogSize = 16 * 8 * 1024 * 1024 // 128MB

// Config tunes the durable layer. The zero value is the production
// configuration — a fully durable, fsync-per-append WAL
type Config struct {
	Dir        string   // WAL directory; empty means walDirectory
	MaxSegSize int64    // segment roll threshold; 0 means maxLogSize
	Mode       wal.Mode // durability mode; zero value is wal.ModeSync
	// CheckpointThreshold is how many segments must accumulate before a
	// checkpoint runs; 0 means wal.DefaultCheckpointThreshold. Together with
	// MaxSegSize this sets how much log is written between checkpoints —
	// the pair a benchmark turns down to exercise the checkpoint path.
	CheckpointThreshold int
}

func (c Config) withDefaults() Config {
	if c.Dir == "" {
		c.Dir = walDirectory
	}
	if c.MaxSegSize == 0 {
		c.MaxSegSize = maxLogSize
	}
	if c.CheckpointThreshold == 0 {
		c.CheckpointThreshold = wal.DefaultCheckpointThreshold
	}
	return c
}

func NewCoordinator() (*Coordinator, error) {
	return NewCoordinatorWithConfig(Config{})
}

func NewCoordinatorWithConfig(cfg Config) (*Coordinator, error) {
	cfg = cfg.withDefaults()
	waLog, err := wal.OpenWithOptions(cfg.Dir, wal.Options{
		MaxSegSize:          cfg.MaxSegSize,
		Mode:                cfg.Mode,
		CheckpointThreshold: cfg.CheckpointThreshold,
	})
	if err != nil {
		return nil, err
	}
	return &Coordinator{
		cat:  catalog.NewCatalog(),
		deli: delivery.NewDelivery(),
		log:  waLog,
	}, nil
}

// Mode reports the durability mode the WAL is running in.
func (c *Coordinator) Mode() wal.Mode { return c.log.Mode() }

func (c *Coordinator) ReplayLog() ([]string,
	map[string]*delivery.DeliveryQueueInfo, error) {

	// Don't allow any modifications while WAL is being replayed
	c.mu.Lock()
	defer c.mu.Unlock()

	records, err := c.log.ReadAll()
	if err != nil {
		return nil, nil, err
	}

	for _, rec := range records {
		opType := rec.OpType

		switch opType {
		case record.OpEnqueue:
			err = c.handleEnqueue(*rec)
			if err != nil {
				return nil, nil, err
			}
		case record.OpAck:
			err = c.deli.ProcessAck(*rec)
			if err != nil {
				return nil, nil, err
			}
		default:
			err = c.cat.ProcessRecord(*rec)
			if err != nil {
				return nil, nil, err
			}
		}

		// Delivery needs to be informed if a queue is removed
		if opType == record.OpDeleteQueue {
			c.deli.DeleteQueueMessages(rec.QueueName)
		}

	}
	catalogSnapshot := c.cat.ReturnQueueNames()
	deliveryResults := c.deli.YieldDeliveryData()
	c.deli = nil
	return catalogSnapshot, deliveryResults, nil
}

func (c *Coordinator) AllQueueInfo() []catalog.QueueInfo {
	return c.cat.AllQueueInfo()
}

// Close releases the WAL's file handles
func (c *Coordinator) Close() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.log.Close()
}

func (c *Coordinator) handleEnqueue(rec record.Record) error {
	return c.deli.ProcessEnqueue(rec)
}

// append wraps log appends with shared logic, such as checkpointing
func (c *Coordinator) append(rec *record.Record) (uint64, error) {
	lsn, err := c.log.Append(rec)
	if err != nil {
		return 0, err
	}
	c.maybeCheckpoint()
	return lsn, nil
}

// The following methods are exposed to the Broker to call to write down to the WAL
// And to store Queue/Sub Data in Catalog

func (c *Coordinator) CreateQueue(queueName string) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	rec := &record.Record{
		OpType:    record.OpCreateQueue,
		QueueName: queueName,
	}
	if _, err := c.append(rec); err != nil {
		return err
	}
	return c.cat.ProcessRecord(*rec)
}

func (c *Coordinator) DeleteQueue(queueName string) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if !c.cat.QueueExists(queueName) {
		return fmt.Errorf("queue %s not found", queueName)
	}

	rec := &record.Record{
		OpType:    record.OpDeleteQueue,
		QueueName: queueName,
	}
	if _, err := c.append(rec); err != nil {
		return err
	}
	return c.cat.ProcessRecord(*rec)
}

func (c *Coordinator) UpdateSubPolicy(queueName string, policy model.SubPolicy) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if !c.cat.QueueExists(queueName) {
		return fmt.Errorf("queue %s not found", queueName)
	}

	payload, err := model.EncodeSubPolicy(policy)
	if err != nil {
		return err
	}
	rec := &record.Record{
		OpType:    record.OpUpdateSubPolicy,
		QueueName: queueName,
		Payload:   payload,
	}
	if _, err := c.append(rec); err != nil {
		return err
	}
	return c.cat.ProcessRecord(*rec)
}

func (c *Coordinator) DeleteSubPolicy(queueName string, subName string) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if !c.cat.QueueExists(queueName) {
		return fmt.Errorf("queue %s not found", queueName)
	}

	payload, err := model.EncodeSubPolicy(model.SubPolicy{SubName: subName})
	if err != nil {
		return err
	}
	rec := &record.Record{
		OpType:    record.OpDeleteSubPolicy,
		QueueName: queueName,
		Payload:   payload,
	}
	if _, err := c.append(rec); err != nil {
		return err
	}
	return c.cat.ProcessRecord(*rec)
}

func (c *Coordinator) Enqueue(queueName string, msgId string, content string) (map[string]model.SubPolicy, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if !c.cat.QueueExists(queueName) {
		return nil, fmt.Errorf("queue %s not found", queueName)
	}

	subList, ok := c.cat.FetchQueueSubList(queueName)
	if !ok {
		// Queue existed moments ago under the same lock — this shouldn't happen
		return nil, fmt.Errorf("queue %s disappeared during enqueue", queueName)
	}

	payload, err := model.EncodeEnqueue(model.Enqueue{MsgId: msgId, MsgContent: content, SubList: subList})
	if err != nil {
		return nil, err
	}
	rec := &record.Record{
		OpType:    record.OpEnqueue,
		QueueName: queueName,
		Payload:   payload,
	}
	if _, err := c.append(rec); err != nil {
		return nil, err
	}
	return subList, nil
}

func (c *Coordinator) Ack(queueName string, msgId string, subName string) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	payload, err := model.EncodeAck(model.Ack{MsgId: msgId, SubName: subName})
	if err != nil {
		return err
	}
	rec := &record.Record{
		OpType:    record.OpAck,
		QueueName: queueName,
		Payload:   payload,
	}
	_, err = c.append(rec)
	return err
}
