package delivery

// Enqueue contains msgId and msgContents
import (
	"durable-mq/catalog"
	"encoding/json"
)

type Enqueue struct {
	MsgId      string                       `json:"MsgId"`
	MsgContent string                       `json:"MsgContent"`
	SubList    map[string]catalog.SubPolicy `json:"SubList"`
}

func EncodeEnqueue(p Enqueue) ([]byte, error) {
	return json.Marshal(p)
}

func DecodeEnqueue(b []byte) (Enqueue, error) {
	var p Enqueue
	if err := json.Unmarshal(b, &p); err != nil {
		return Enqueue{}, err
	}
	return p, nil
}
