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

	// Counts cover the measurement window only (warm-up excluded).
	Attempted uint64 `json:"Attempted"`
	Accepted  uint64 `json:"Accepted"`
	Failed    uint64 `json:"Failed"`

	// AcceptedPerSec is the broker's achieved throughput: accepted publishes
	// divided by the measurement window. Compare against OfferedRate to see
	// whether the run was saturated.
	AcceptedPerSec float64 `json:"AcceptedPerSec"`
	Latency        Latency `json:"Latency"`

	// Delivered is what the sink actually received during the run, if it was
	// reachable. Publishes are acknowledged before delivery happens, so this
	// legitimately lags Accepted at the moment the run stops.
	Delivered      uint64 `json:"Delivered"`
	DeliveredKnown bool   `json:"DeliveredKnown"`
}

// summarize filters out the warm-up period and computes the percentiles over
// what remains.
func summarize(samples []sample, warmup, total time.Duration) (Latency, uint64, uint64, float64) {
	lat := make([]time.Duration, 0, len(samples))
	var attempted, accepted uint64
	var sum time.Duration

	for _, s := range samples {
		if s.scheduled < warmup {
			continue // still warming up: connections cold, caches empty
		}
		attempted++
		if !s.ok {
			continue
		}
		accepted++
		lat = append(lat, s.latency)
		sum += s.latency
	}

	window := (total - warmup).Seconds()
	if window <= 0 {
		window = math.NaN()
	}

	if len(lat) == 0 {
		return Latency{}, attempted, accepted, 0
	}
	sort.Slice(lat, func(i, j int) bool { return lat[i] < lat[j] })

	return Latency{
		Mean: ms(sum / time.Duration(len(lat))),
		P50:  ms(percentile(lat, 50)),
		P90:  ms(percentile(lat, 90)),
		P99:  ms(percentile(lat, 99)),
		Max:  ms(lat[len(lat)-1]),
	}, attempted, accepted, float64(accepted) / window
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
