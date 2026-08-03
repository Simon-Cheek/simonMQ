package model

import "encoding/json"

type SubPolicy struct {
	SubName         string `json:"SubName"`
	SubURL          string `json:"SubURL"` // URL to call POST /queue/message on
	NumberOfRetries int    `json:"NumberOfRetries"`
}

func EncodeSubPolicy(p SubPolicy) ([]byte, error) {
	return json.Marshal(p)
}

func DecodeSubPolicy(b []byte) (SubPolicy, error) {
	var p SubPolicy
	if err := json.Unmarshal(b, &p); err != nil {
		return SubPolicy{}, err
	}
	return p, nil
}
