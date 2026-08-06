package main

import (
	"math"
	"sort"
	"time"
)

// sample is one attempted publish. scheduled is when the request was *due*,
// not when it was actually issued — see the comment on latency below.
type sample struct {
	scheduled time.Duration // offset from run start
	latency   time.Duration // completion minus scheduled
	ok        bool
}

// Latency holds the percentile summary of one run, in milliseconds.
type Latency struct {
	Mean float64 `json:"MeanMs"`
	P50  float64 `json:"P50Ms"`
	P90  float64 `json:"P90Ms"`
	P99  float64 `json:"P99Ms"`
	Max  float64 `json:"MaxMs"`
}

// Result is one (arm, repetition) measurement, written out as JSON so the
// aggregate tool can combine repetitions without re-running anything.
type Result struct {
	Arm       string    `json:"Arm"`
	Rep       int       `json:"Rep"`
	Host      string    `json:"Host"`
	StartedAt time.Time `json:"StartedAt"`

	Broker              string  `json:"Broker"`
	Queues              int     `json:"Queues"`
	PublishersPerQueue  int     `json:"PublishersPerQueue"`
	SubscribersPerQueue int     `json:"SubscribersPerQueue"`
	TargetRatePerPub    float64 `json:"TargetRatePerPublisherPerSec"`
	OfferedRate         float64 `json:"OfferedRatePerSec"`
	PayloadBytes        int     `json:"PayloadBytes"`
	DurationSec         float64 `json:"DurationSec"`
	WarmupSec           float64 `json:"WarmupSec"`

	// Attempted/Accepted/Failed count publishes *scheduled* in the measurement
	// window (warm-up excluded), which is the same set the latency figures
	// describe. They may complete after the window closes.
	Attempted uint64 `json:"Attempted"`
	Accepted  uint64 `json:"Accepted"`
	Failed    uint64 `json:"Failed"`

	// CompletedInWindow counts publishes that actually *finished* inside the
	// window, and is the numerator behind AcceptedPerSec. It falls below
	// Accepted exactly when the broker could not keep up and spilled work
	// past the end of the run.
	CompletedInWindow uint64  `json:"CompletedInWindow"`
	AcceptedPerSec    float64 `json:"AcceptedPerSec"`

	// ElapsedSec is when the last publish finally completed, measured from the
	// start of the run. Materially larger than DurationSec means the broker
	// was still draining after the load stopped.
	ElapsedSec float64 `json:"ElapsedSec"`

	Latency Latency `json:"Latency"`

	// Delivered is what the sink actually received during the run, if it was
	// reachable. Publishes are acknowledged before delivery happens, so this
	// legitimately lags Accepted at the moment the run stops.
	Delivered      uint64 `json:"Delivered"`
	DeliveredKnown bool   `json:"DeliveredKnown"`
}

// summary is what one run reduces to. Latency and throughput are deliberately
// derived from different subsets of the samples — see summarize.
type summary struct {
	Latency           Latency
	Attempted         uint64
	Accepted          uint64
	CompletedInWindow uint64
	AcceptedPerSec    float64
	ElapsedSec        float64
}

// summarize reduces one run's samples, using two different filters:
//
//   - Latency covers requests *scheduled* inside the window, however long they
//     took. Filtering these by completion instead would discard precisely the
//     slowest requests and report a saturated broker as fast.
//   - Throughput counts completions that *landed* inside the window. Dividing
//     everything scheduled in the window by the window length — which this
//     used to do — reports the offered rate rather than the achieved one the
//     moment a broker falls behind, because the work spilled past the window
//     but the divisor didn't grow to match.
//
// For an unsaturated run the two agree, since almost everything scheduled in
// the window also finishes there. They diverge exactly when it matters.
func summarize(samples []sample, warmup, total time.Duration) summary {
	lat := make([]time.Duration, 0, len(samples))
	var out summary
	var sum time.Duration
	var lastCompletion time.Duration

	for _, s := range samples {
		completion := s.scheduled + s.latency
		if completion > lastCompletion {
			lastCompletion = completion
		}

		if s.ok && completion >= warmup && completion < total {
			out.CompletedInWindow++
		}

		if s.scheduled < warmup {
			continue // still warming up: connections cold, caches empty
		}
		out.Attempted++
		if !s.ok {
			continue
		}
		out.Accepted++
		lat = append(lat, s.latency)
		sum += s.latency
	}

	out.ElapsedSec = lastCompletion.Seconds()

	window := (total - warmup).Seconds()
	if window <= 0 {
		window = math.NaN()
	}
	out.AcceptedPerSec = float64(out.CompletedInWindow) / window

	if len(lat) == 0 {
		return out
	}
	sort.Slice(lat, func(i, j int) bool { return lat[i] < lat[j] })

	out.Latency = Latency{
		Mean: ms(sum / time.Duration(len(lat))),
		P50:  ms(percentile(lat, 50)),
		P90:  ms(percentile(lat, 90)),
		P99:  ms(percentile(lat, 99)),
		Max:  ms(lat[len(lat)-1]),
	}
	return out
}

// percentile uses nearest-rank on the sorted slice. Samples are kept raw
// rather than bucketed, so these are exact rather than approximations.
func percentile(sorted []time.Duration, p float64) time.Duration {
	if len(sorted) == 0 {
		return 0
	}
	rank := int(math.Ceil(p / 100 * float64(len(sorted))))
	if rank < 1 {
		rank = 1
	}
	if rank > len(sorted) {
		rank = len(sorted)
	}
	return sorted[rank-1]
}

func ms(d time.Duration) float64 {
	return float64(d) / float64(time.Millisecond)
}
