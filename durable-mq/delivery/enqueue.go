package delivery

// Enqueue contains msgId and msgContents
import "encoding/json"

type Enqueue struct {
	MsgId      string `json:"MsgId"`
	MsgContent string `json:"MsgContent"`
}

func encodeEnqueue(p Enqueue) ([]byte, error) {
	return json.Marshal(p)
}

func decodeEnqueue(b []byte) (Enqueue, error) {
	var p Enqueue
	if err := json.Unmarshal(b, &p); err != nil {
		return Enqueue{}, err
	}
	return p, nil
}
