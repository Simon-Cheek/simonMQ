// Package verify turns a run's publish outcomes and the sink's delivery
// records into a verdict.
//
// The rules follow from what dist-mq actually promises, which is narrower than
// "nothing is ever lost":
//
//   - An accepted write is on a quorum of disks, so every subscriber
//     registered at enqueue time must eventually see it. This is the only
//     assertion with teeth, and it holds only because the sink never fails —
//     a subscriber that exhausts its retries is recorded as settled without
//     having received anything, which would be indistinguishable from loss.
//   - An ambiguous write may or may not have committed. Nothing may be
//     asserted about it in either direction. Counting one as lost invents a
//     durability bug; counting one as delivered hides a real one.
//   - A rejected write definitively did not commit, so delivering it would be
//     the cluster inventing a message.
//   - Duplicates are permitted. Delivery is at-least-once and a leadership
//     change is entitled to produce them, so they are counted, never failed.
package verify

import (
	"fmt"
	"sort"
	"strings"

	"dist-mq/e2e-tests/client"
)

// Publication is one token and what the cluster said about it.
type Publication struct {
	Token   string
	Outcome client.Outcome
}

// Miss is an accepted message a subscriber never received.
type Miss struct {
	Subscriber string
	Token      string
}

type Report struct {
	Accepted  int
	Ambiguous int
	Rejected  int

	// Missing is the failure that matters: acknowledged, then lost.
	Missing []Miss

	// Delivered but never published at all.
	Phantom []string

	// Delivered despite a definitive rejection.
	RejectedButDelivered []string

	// Informational. Duplicates are allowed; AmbiguousDelivered is how many
	// unassertable writes turned out to have committed, which is useful for
	// understanding a chaos run without ever being a pass or fail condition.
	Duplicates         int
	AmbiguousDelivered int
}

// Check compares what was published against what each subscriber recorded.
// records maps subscriber name to token to delivery count, which is the shape
// sink.Records carries.
func Check(pubs []Publication, records map[string]map[string]int, subscribers []string) Report {
	var rep Report

	accepted := make(map[string]struct{}, len(pubs))
	ambiguous := make(map[string]struct{})
	rejected := make(map[string]struct{})

	for _, p := range pubs {
		switch p.Outcome {
		case client.Accepted:
			rep.Accepted++
			accepted[p.Token] = struct{}{}
		case client.Ambiguous:
			rep.Ambiguous++
			ambiguous[p.Token] = struct{}{}
		case client.Rejected:
			rep.Rejected++
			rejected[p.Token] = struct{}{}
		}
	}

	// Every accepted token owes an appearance under every subscriber. A
	// subscriber missing from records entirely fails the same way a subscriber
	// missing one token does, which is what makes a sink that never started
	// impossible to mistake for a clean run.
	for _, sub := range subscribers {
		seen := records[sub]
		for token := range accepted {
			if seen[token] == 0 {
				rep.Missing = append(rep.Missing, Miss{Subscriber: sub, Token: token})
			}
		}
	}

	deliveredAmbiguous := make(map[string]struct{})
	for _, sub := range subscribers {
		for token, count := range records[sub] {
			if count > 1 {
				rep.Duplicates += count - 1
			}
			switch {
			case isIn(accepted, token):
			case isIn(ambiguous, token):
				deliveredAmbiguous[token] = struct{}{}
			case isIn(rejected, token):
				rep.RejectedButDelivered = append(rep.RejectedButDelivered, token)
			default:
				rep.Phantom = append(rep.Phantom, token)
			}
		}
	}
	rep.AmbiguousDelivered = len(deliveredAmbiguous)

	sortMisses(rep.Missing)
	sort.Strings(rep.Phantom)
	sort.Strings(rep.RejectedButDelivered)
	return rep
}

// Err is the pass/fail verdict. Duplicates and delivered-ambiguous writes are
// deliberately absent: both are permitted by the delivery contract.
func (r Report) Err() error {
	var problems []string
	if len(r.Missing) > 0 {
		problems = append(problems, fmt.Sprintf("%d accepted message(s) never delivered, e.g. %s",
			len(r.Missing), r.Missing[0]))
	}
	if len(r.Phantom) > 0 {
		problems = append(problems, fmt.Sprintf("%d delivered message(s) were never published, e.g. %q",
			len(r.Phantom), r.Phantom[0]))
	}
	if len(r.RejectedButDelivered) > 0 {
		problems = append(problems, fmt.Sprintf("%d rejected message(s) were delivered anyway, e.g. %q",
			len(r.RejectedButDelivered), r.RejectedButDelivered[0]))
	}
	if len(problems) == 0 {
		return nil
	}
	return fmt.Errorf("%s", strings.Join(problems, "; "))
}

func (r Report) String() string {
	return fmt.Sprintf(
		"accepted=%d ambiguous=%d rejected=%d | missing=%d phantom=%d rejected-but-delivered=%d | duplicates=%d ambiguous-delivered=%d",
		r.Accepted, r.Ambiguous, r.Rejected,
		len(r.Missing), len(r.Phantom), len(r.RejectedButDelivered),
		r.Duplicates, r.AmbiguousDelivered)
}

func (m Miss) String() string { return m.Subscriber + " never received " + m.Token }

func isIn(set map[string]struct{}, key string) bool {
	_, ok := set[key]
	return ok
}

func sortMisses(m []Miss) {
	sort.Slice(m, func(i, j int) bool {
		if m[i].Subscriber != m[j].Subscriber {
			return m[i].Subscriber < m[j].Subscriber
		}
		return m[i].Token < m[j].Token
	})
}
