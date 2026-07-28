package delivery

import "encoding/json"

type Ack struct {
	MsgId   string `json:"MsgId"`
	SubName string `json:"SubName"`
}

func encodeAck(p Ack) ([]byte, error) {
	return json.Marshal(p)
}

func decodeAck(b []byte) (Ack, error) {
	var p Ack
	if err := json.Unmarshal(b, &p); err != nil {
		return Ack{}, err
	}
	return p, nil
}
