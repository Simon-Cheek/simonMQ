package model

// QueueInfo is the observable state of a single queue: its registered
// subscribers plus the messages still holding unacked subscribers.
type QueueInfo struct {
	Name        string               `json:"Name"`
	SubPolicies map[string]SubPolicy `json:"SubPolicies"`
	Messages    []MessageInfo        `json:"Messages"`
}

type MessageInfo struct {
	MsgID     string               `json:"MsgId"`
	Payload   string               `json:"Payload"`
	SubList   map[string]SubPolicy `json:"SubList"`
	AckedSubs []string             `json:"AckedSubs"`
}

// PendingSubs is a helper method to calculate remaining Subs (UnAcked)
func (m MessageInfo) PendingSubs() map[string]SubPolicy {
	acked := make(map[string]struct{}, len(m.AckedSubs))
	for _, name := range m.AckedSubs {
		acked[name] = struct{}{}
	}

	pending := make(map[string]SubPolicy, len(m.SubList))
	for name, policy := range m.SubList {
		if _, done := acked[name]; done {
			continue
		}
		pending[name] = policy
	}
	return pending
}
