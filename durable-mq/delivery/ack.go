package delivery

import "encoding/json"

type Ack struct {
	MsgId   string `json:"MsgId"`
	SubName string `json:"SubName"`
}

func EncodeAck(p Ack) ([]byte, error) {
	return json.Marshal(p)
}

func DecodeAck(b []byte) (Ack, error) {
	var p Ack
	if err := json.Unmarshal(b, &p); err != nil {
		return Ack{}, err
	}
	return p, nil
}
