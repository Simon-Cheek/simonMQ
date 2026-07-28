package catalog

import "encoding/json"

type SubPolicy struct {
	SubName         string `json:"subName"`
	SubURL          string `json:"subURL"` // URL to call POST /queue/message on
	NumberOfRetries int    `json:"numberOfRetries"`
}

func encodeSubPolicy(p SubPolicy) ([]byte, error) {
	return json.Marshal(p)
}

func decodeSubPolicy(b []byte) (SubPolicy, error) {
	var p SubPolicy
	if err := json.Unmarshal(b, &p); err != nil {
		return SubPolicy{}, err
	}
	return p, nil
}
