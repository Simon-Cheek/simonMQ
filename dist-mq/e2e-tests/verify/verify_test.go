package verify_test

import (
	"testing"

	"dist-mq/e2e-tests/client"
	"dist-mq/e2e-tests/verify"
)

func pub(token string, outcome client.Outcome) verify.Publication {
	return verify.Publication{Token: token, Outcome: outcome}
}

var subs = []string{"sub-0", "sub-1"}

func TestCleanRunPasses(t *testing.T) {
	rep := verify.Check(
		[]verify.Publication{pub("a", client.Accepted), pub("b", client.Accepted)},
		map[string]map[string]int{
			"sub-0": {"a": 1, "b": 1},
			"sub-1": {"a": 1, "b": 1},
		}, subs)

	if err := rep.Err(); err != nil {
		t.Fatalf("clean run failed: %v (%s)", err, rep)
	}
	if rep.Accepted != 2 {
		t.Fatalf("accepted = %d, want 2", rep.Accepted)
	}
}

// The failure the whole suite exists to catch.
func TestAcceptedButUndeliveredFails(t *testing.T) {
	rep := verify.Check(
		[]verify.Publication{pub("a", client.Accepted)},
		map[string]map[string]int{
			"sub-0": {"a": 1},
			"sub-1": {}, // acknowledged, then lost
		}, subs)

	if rep.Err() == nil {
		t.Fatal("an accepted message that reached only one subscriber passed")
	}
	if len(rep.Missing) != 1 || rep.Missing[0].Subscriber != "sub-1" {
		t.Fatalf("missing = %v, want one miss on sub-1", rep.Missing)
	}
}

// A sink that never came up must not look like a clean run.
func TestSubscriberWithNoRecordsAtAllFails(t *testing.T) {
	rep := verify.Check(
		[]verify.Publication{pub("a", client.Accepted), pub("b", client.Accepted)},
		map[string]map[string]int{}, subs)

	if rep.Err() == nil {
		t.Fatal("a run where nothing was delivered passed")
	}
	if len(rep.Missing) != 4 {
		t.Fatalf("missing = %d, want 4 (2 tokens x 2 subscribers)", len(rep.Missing))
	}
}

// Ambiguous writes are unassertable in both directions. These two tests are
// the pair that keeps a chaos run honest.
func TestAmbiguousUndeliveredPasses(t *testing.T) {
	rep := verify.Check(
		[]verify.Publication{pub("a", client.Accepted), pub("lost", client.Ambiguous)},
		map[string]map[string]int{
			"sub-0": {"a": 1},
			"sub-1": {"a": 1},
		}, subs)

	if err := rep.Err(); err != nil {
		t.Fatalf("an undelivered ambiguous write failed the run: %v", err)
	}
	if rep.Ambiguous != 1 {
		t.Fatalf("ambiguous = %d, want 1", rep.Ambiguous)
	}
}

func TestAmbiguousDeliveredPassesAndIsCounted(t *testing.T) {
	rep := verify.Check(
		[]verify.Publication{pub("maybe", client.Ambiguous)},
		map[string]map[string]int{
			"sub-0": {"maybe": 1},
			"sub-1": {"maybe": 1},
		}, subs)

	if err := rep.Err(); err != nil {
		t.Fatalf("a delivered ambiguous write failed the run: %v", err)
	}
	if rep.AmbiguousDelivered != 1 {
		t.Fatalf("ambiguousDelivered = %d, want 1", rep.AmbiguousDelivered)
	}
}

// At-least-once permits these, so they are reported and never failed.
func TestDuplicatesAreCountedNotFailed(t *testing.T) {
	rep := verify.Check(
		[]verify.Publication{pub("a", client.Accepted)},
		map[string]map[string]int{
			"sub-0": {"a": 3},
			"sub-1": {"a": 1},
		}, subs)

	if err := rep.Err(); err != nil {
		t.Fatalf("duplicates failed the run: %v", err)
	}
	if rep.Duplicates != 2 {
		t.Fatalf("duplicates = %d, want 2", rep.Duplicates)
	}
}

func TestPhantomDeliveryFails(t *testing.T) {
	rep := verify.Check(
		[]verify.Publication{pub("a", client.Accepted)},
		map[string]map[string]int{
			"sub-0": {"a": 1, "invented": 1},
			"sub-1": {"a": 1},
		}, subs)

	if rep.Err() == nil {
		t.Fatal("a delivery that was never published passed")
	}
	if len(rep.Phantom) != 1 || rep.Phantom[0] != "invented" {
		t.Fatalf("phantom = %v, want [invented]", rep.Phantom)
	}
}

// A definitive rejection means nothing was proposed, so delivery would be the
// cluster inventing a message.
func TestRejectedButDeliveredFails(t *testing.T) {
	rep := verify.Check(
		[]verify.Publication{pub("no", client.Rejected)},
		map[string]map[string]int{
			"sub-0": {"no": 1},
			"sub-1": {},
		}, subs)

	if rep.Err() == nil {
		t.Fatal("a rejected message that was delivered anyway passed")
	}
	if len(rep.RejectedButDelivered) != 1 {
		t.Fatalf("rejectedButDelivered = %v, want one entry", rep.RejectedButDelivered)
	}
}

func TestRejectedAndUndeliveredPasses(t *testing.T) {
	rep := verify.Check(
		[]verify.Publication{pub("a", client.Accepted), pub("no", client.Rejected)},
		map[string]map[string]int{
			"sub-0": {"a": 1},
			"sub-1": {"a": 1},
		}, subs)

	if err := rep.Err(); err != nil {
		t.Fatalf("a correctly rejected write failed the run: %v", err)
	}
	if rep.Rejected != 1 {
		t.Fatalf("rejected = %d, want 1", rep.Rejected)
	}
}

func TestEmptyRunPasses(t *testing.T) {
	if err := verify.Check(nil, nil, subs).Err(); err != nil {
		t.Fatalf("an empty run failed: %v", err)
	}
}
