package command

import (
	"encoding/json"
	"fmt"

	"dist-mq/model"
)

type Type uint8

const (
	CreateQueue Type = iota + 1
	DeleteQueue
	PutSubPolicy
	DeleteSubPolicy
	Enqueue
	Ack
)

func (t Type) String() string {
	switch t {
	case CreateQueue:
		return "CreateQueue"
	case DeleteQueue:
		return "DeleteQueue"
	case PutSubPolicy:
		return "PutSubPolicy"
	case DeleteSubPolicy:
		return "DeleteSubPolicy"
	case Enqueue:
		return "Enqueue"
	case Ack:
		return "Ack"
	default:
		return fmt.Sprintf("Type(%d)", uint8(t))
	}
}

// Command is one entry in the replicated log — the serialized form of a single
// storage mutation
type Command struct {
	Type      Type                       `json:"Type"`
	QueueName string                     `json:"QueueName"`
	MsgID     string                     `json:"MsgId,omitempty"`
	Payload   string                     `json:"Payload,omitempty"`
	SubName   string                     `json:"SubName,omitempty"`
	Policy    model.SubPolicy            `json:"Policy,omitzero"`
	SubNames  []string                   `json:"SubNames,omitempty"`
	SubList   map[string]model.SubPolicy `json:"SubList,omitempty"`
}

func NewCreateQueue(queueName string) Command {
	return Command{Type: CreateQueue, QueueName: queueName}
}

func NewDeleteQueue(queueName string) Command {
	return Command{Type: DeleteQueue, QueueName: queueName}
}

func NewPutSubPolicy(queueName string, policy model.SubPolicy) Command {
	return Command{Type: PutSubPolicy, QueueName: queueName, Policy: policy}
}

func NewDeleteSubPolicy(queueName, subName string) Command {
	return Command{Type: DeleteSubPolicy, QueueName: queueName, SubName: subName}
}

func NewEnqueue(queueName, msgID, payload string, subList map[string]model.SubPolicy) Command {
	subs := make(map[string]model.SubPolicy, len(subList))
	for name, policy := range subList {
		subs[name] = policy
	}
	return Command{
		Type:      Enqueue,
		QueueName: queueName,
		MsgID:     msgID,
		Payload:   payload,
		SubList:   subs,
	}
}

func NewAck(queueName, msgID string, subNames []string) Command {
	names := make([]string, len(subNames))
	copy(names, subNames)
	return Command{Type: Ack, QueueName: queueName, MsgID: msgID, SubNames: names}
}

func Encode(cmd Command) ([]byte, error) {
	if err := cmd.validate(); err != nil {
		return nil, err
	}
	return json.Marshal(cmd)
}

func Decode(data []byte) (Command, error) {
	if len(data) == 0 {
		return Command{}, fmt.Errorf("decode command: empty entry")
	}

	var cmd Command
	if err := json.Unmarshal(data, &cmd); err != nil {
		return Command{}, fmt.Errorf("decode command: %w", err)
	}
	if err := cmd.validate(); err != nil {
		return Command{}, fmt.Errorf("decode command: %w", err)
	}
	if cmd.Type == Ack && cmd.SubNames == nil {
		cmd.SubNames = []string{}
	}
	return cmd, nil
}

// validate checks only that the command is structurally usable
func (c Command) validate() error {
	if c.QueueName == "" {
		return fmt.Errorf("%s: empty queue name", c.Type)
	}

	switch c.Type {
	case CreateQueue, DeleteQueue:
		return nil
	case PutSubPolicy:
		if c.Policy.SubName == "" {
			return fmt.Errorf("PutSubPolicy: empty subscriber name")
		}
		return nil
	case DeleteSubPolicy:
		if c.SubName == "" {
			return fmt.Errorf("DeleteSubPolicy: empty subscriber name")
		}
		return nil
	case Enqueue:
		// Payload is unchecked: an empty message body is legal content.
		if c.MsgID == "" {
			return fmt.Errorf("Enqueue: empty message id")
		}
		return nil
	case Ack:
		if c.MsgID == "" {
			return fmt.Errorf("Ack: empty message id")
		}
		return nil
	default:
		return fmt.Errorf("unknown command type %d", uint8(c.Type))
	}
}
