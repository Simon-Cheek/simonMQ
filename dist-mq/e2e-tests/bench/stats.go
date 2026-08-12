package bench

import (
	"math"
	"sort"
	"time"

	"dist-mq/e2e-tests/client"
)

// sample is one publish. latency is measured from when the publish was
// scheduled, not from when the request was sent — see Run.
type sample struct {
	offset  time.Duration // from the start of the run
	latency time.Duration
	outcome client.Outcome
}

type Latency struct {
	P50 float64 `json:"P50Ms"`
	P90 float64 `json:"P90Ms"`
	P99 float64 `json:"P99Ms"`
	Max float64 `json:"MaxMs"`
}

type summary struct {
	Attempted      int
	Accepted       int
	Ambiguous      int
	Rejected       int
	AcceptedPerSec float64
	ElapsedSec     float64
	Latency        Latency
}

// summarize drops the warm-up window and reduces what is left. Only accepted
// publishes contribute latency: a rejected write's duration is the cost of
// being turned away, and averaging it in flatters the result.
func summarize(samples []sample, warmup, duration time.Duration) summary {
	var s summary
	latencies := make([]float64, 0, len(samples))
	var first, last time.Duration

	for _, smp := range samples {
		if smp.offset < warmup {
			continue
		}
		s.Attempted++
		if s.Attempted == 1 || smp.offset < first {
			first = smp.offset
		}
		if smp.offset > last {
			last = smp.offset
		}

		switch smp.outcome {
		case client.Accepted:
			s.Accepted++
			latencies = append(latencies, float64(smp.latency)/float64(time.Millisecond))
		case client.Ambiguous:
			s.Ambiguous++
		case client.Rejected:
			s.Rejected++
		}
	}

	s.ElapsedSec = (duration - warmup).Seconds()
	if elapsed := (last - first).Seconds(); elapsed > 0 {
		s.ElapsedSec = elapsed
	}
	if s.ElapsedSec > 0 {
		s.AcceptedPerSec = float64(s.Accepted) / s.ElapsedSec
	}
	s.Latency = percentiles(latencies)
	return s
}

func percentiles(v []float64) Latency {
	if len(v) == 0 {
		return Latency{}
	}
	sort.Float64s(v)
	return Latency{
		P50: quantile(v, 0.50),
		P90: quantile(v, 0.90),
		P99: quantile(v, 0.99),
		Max: v[len(v)-1],
	}
}

// quantile uses nearest-rank, which reports a value that was actually
// observed rather than one interpolated between two that were not.
func quantile(sorted []float64, q float64) float64 {
	if len(sorted) == 0 {
		return 0
	}
	rank := int(math.Ceil(q*float64(len(sorted)))) - 1
	if rank < 0 {
		rank = 0
	}
	if rank >= len(sorted) {
		rank = len(sorted) - 1
	}
	return sorted[rank]
}
