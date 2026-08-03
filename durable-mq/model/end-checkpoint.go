package model

import "encoding/json"

type EndCheckpoint struct {
	FileName     string `json:"FileName"`
	FileChecksum string `json:"FileChecksum"`
}

func EncodeEndCheckpoint(e EndCheckpoint) ([]byte, error) {
	return json.Marshal(e)
}

func DecodeEndCheckpoint(b []byte) (EndCheckpoint, error) {
	var e EndCheckpoint
	if err := json.Unmarshal(b, &e); err != nil {
		return EndCheckpoint{}, err
	}
	return e, nil
}
