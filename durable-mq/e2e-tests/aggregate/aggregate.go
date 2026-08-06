// Command aggregate combines the per-repetition JSON files loadgen writes and
// prints a markdown summary.
//
// Repetitions are reported as median with an interquartile range rather than a
// mean, because a laptop under thermal drift produces occasional slow runs
// that would drag a mean around while leaving the median alone. The IQR is
// printed rather than hidden: a wide spread is information about the
// measurement, and suppressing it is how benchmarks end up misleading.
//
// Every arm is also expressed as a ratio against a baseline arm. Absolute
// throughput is a fact about one machine; the ratio between two arms measured
// back-to-back on that machine is a fact about the system, and it is the
// number that survives being read by someone with different hardware.
package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"sort"
	"strings"
)

type result struct {
	Arm            string  `json:"Arm"`
	Rep            int     `json:"Rep"`
	Host           string  `json:"Host"`
	OfferedRate    float64 `json:"OfferedRatePerSec"`
	AcceptedPerSec float64 `json:"AcceptedPerSec"`
	Accepted       uint64  `json:"Accepted"`
	Failed         uint64  `json:"Failed"`
	Delivered      uint64  `json:"Delivered"`
	DeliveredKnown bool    `json:"DeliveredKnown"`
	Latency        struct {
		P50 float64 `json:"P50Ms"`
		P90 float64 `json:"P90Ms"`
		P99 float64 `json:"P99Ms"`
		Max float64 `json:"MaxMs"`
	} `json:"Latency"`
}

type armStats struct {
	name      string
	reps      int
	thru      []float64
	p50       []float64
	p99       []float64
	failed    uint64
	accepted  uint64
	delivered uint64
	offered   float64
}

func main() {
	dir := flag.String("dir", "results", "directory of result JSON files")
	baseline := flag.String("baseline", "", "arm to express ratios against (default: durable-off if present, else the first arm)")
	order := flag.String("order", "push-mq,durable-off,durable-nosync,durable-sync", "comma-separated arm display order; unlisted arms follow alphabetically")
	flag.Parse()

	paths, err := filepath.Glob(filepath.Join(*dir, "*.json"))
	if err != nil || len(paths) == 0 {
		fmt.Fprintf(os.Stderr, "no result files found in %s\n", *dir)
		os.Exit(1)
	}

	byArm := map[string]*armStats{}
	hosts := map[string]struct{}{}
	for _, p := range paths {
		raw, err := os.ReadFile(p)
		if err != nil {
			fmt.Fprintf(os.Stderr, "reading %s: %v\n", p, err)
			continue
		}
		var r result
		if err := json.Unmarshal(raw, &r); err != nil {
			fmt.Fprintf(os.Stderr, "parsing %s: %v\n", p, err)
			continue
		}
		a, ok := byArm[r.Arm]
		if !ok {
			a = &armStats{name: r.Arm}
			byArm[r.Arm] = a
		}
		a.reps++
		a.thru = append(a.thru, r.AcceptedPerSec)
		a.p50 = append(a.p50, r.Latency.P50)
		a.p99 = append(a.p99, r.Latency.P99)
		a.failed += r.Failed
		a.accepted += r.Accepted
		a.delivered += r.Delivered
		a.offered = r.OfferedRate
		hosts[r.Host] = struct{}{}
	}

	arms := sortArms(byArm, *order)
	base := pickBaseline(byArm, arms, *baseline)

	hostList := make([]string, 0, len(hosts))
	for h := range hosts {
		hostList = append(hostList, h)
	}
	sort.Strings(hostList)

	fmt.Printf("# Benchmark summary\n\n")
	fmt.Printf("Load generator host(s): %s\n\n", strings.Join(hostList, ", "))
	fmt.Printf("Offered load: %.0f msg/s. Baseline arm: `%s`.\n", byArm[arms[0]].offered, base)
	fmt.Printf("Throughput and latency are medians across repetitions; brackets are the interquartile range.\n\n")

	fmt.Printf("| Arm | Reps | Accepted msg/s (IQR) | vs %s | p50 ms (IQR) | p99 ms (IQR) | Failed | Saturated |\n", base)
	fmt.Printf("|---|---|---|---|---|---|---|---|\n")

	baseThru := median(byArm[base].thru)
	for _, name := range arms {
		a := byArm[name]
		mt := median(a.thru)
		ratio := "—"
		if name != base && baseThru > 0 {
			ratio = fmt.Sprintf("%.2fx", mt/baseThru)
		}
		// Saturation shows up two different ways and both have to be checked.
		// The publish path saturates when the broker cannot accept at the
		// offered rate. The delivery path saturates while publishes are still
		// being accepted normally — the broker returns 202 promptly and then
		// falls behind pushing to subscribers, so accepted/offered looks
		// healthy and only the delivery backlog gives it away. Either way the
		// latency figures on that row describe a queue building up, not the
		// broker's service time, and must not be compared against an
		// unsaturated arm's.
		var why []string
		if a.offered > 0 && mt < a.offered*0.95 {
			why = append(why, "publish")
		}
		if a.accepted > 0 && float64(a.delivered)/float64(a.accepted) < 0.90 {
			why = append(why, "delivery")
		}
		sat := "no"
		if len(why) > 0 {
			sat = "**" + strings.Join(why, "+") + "**"
		}
		fmt.Printf("| `%s` | %d | %.0f [%.0f–%.0f] | %s | %.2f [%.2f–%.2f] | %.2f [%.2f–%.2f] | %d | %s |\n",
			name, a.reps,
			mt, q1(a.thru), q3(a.thru),
			ratio,
			median(a.p50), q1(a.p50), q3(a.p50),
			median(a.p99), q1(a.p99), q3(a.p99),
			a.failed, sat)
	}

	fmt.Printf("\n## Delivery\n\n")
	fmt.Printf("| Arm | Accepted | Delivered | Ratio |\n|---|---|---|---|\n")
	for _, name := range arms {
		a := byArm[name]
		r := "—"
		if a.accepted > 0 {
			r = fmt.Sprintf("%.3f", float64(a.delivered)/float64(a.accepted))
		}
		fmt.Printf("| `%s` | %d | %d | %s |\n", name, a.accepted, a.delivered, r)
	}
	fmt.Printf("\nBoth counts cover the measurement window only. A ratio slightly below 1 is expected — publishes are acknowledged before delivery happens, so whatever is in flight when the run stops is never counted. A ratio far below 1 means the delivery path could not keep up with the publish path, and any latency figure from that arm is a queueing delay rather than a service time.\n")

	if anyWide(byArm) {
		fmt.Printf("\n> Some arms show an IQR wider than 10%% of their median. Treat those as noisy and add repetitions before drawing conclusions from them.\n")
	}
}

func sortArms(byArm map[string]*armStats, order string) []string {
	rank := map[string]int{}
	for i, name := range strings.Split(order, ",") {
		rank[strings.TrimSpace(name)] = i
	}
	arms := make([]string, 0, len(byArm))
	for name := range byArm {
		arms = append(arms, name)
	}
	sort.Slice(arms, func(i, j int) bool {
		ri, oki := rank[arms[i]]
		rj, okj := rank[arms[j]]
		if oki && okj {
			return ri < rj
		}
		if oki != okj {
			return oki
		}
		return arms[i] < arms[j]
	})
	return arms
}

func pickBaseline(byArm map[string]*armStats, arms []string, want string) string {
	if want != "" {
		if _, ok := byArm[want]; ok {
			return want
		}
		fmt.Fprintf(os.Stderr, "baseline %q not present in results, falling back\n", want)
	}
	if _, ok := byArm["durable-off"]; ok {
		return "durable-off"
	}
	return arms[0]
}

func anyWide(byArm map[string]*armStats) bool {
	for _, a := range byArm {
		m := median(a.thru)
		if m > 0 && (q3(a.thru)-q1(a.thru))/m > 0.10 {
			return true
		}
	}
	return false
}

func median(xs []float64) float64 { return quantile(xs, 0.50) }
func q1(xs []float64) float64     { return quantile(xs, 0.25) }
func q3(xs []float64) float64     { return quantile(xs, 0.75) }

// quantile uses linear interpolation between the two nearest ranks, which is
// better behaved than nearest-rank at the small repetition counts these runs
// produce.
func quantile(xs []float64, p float64) float64 {
	if len(xs) == 0 {
		return math.NaN()
	}
	s := append([]float64(nil), xs...)
	sort.Float64s(s)
	if len(s) == 1 {
		return s[0]
	}
	pos := p * float64(len(s)-1)
	lo := int(math.Floor(pos))
	hi := int(math.Ceil(pos))
	if lo == hi {
		return s[lo]
	}
	return s[lo] + (s[hi]-s[lo])*(pos-float64(lo))
}
