// Command aggregate reduces per-run benchmark JSON to a markdown summary.
//
// Repetitions are summarised by their median rather than their mean: one
// unlucky run — a slow reschedule, a snapshot landing mid-measurement — moves
// a mean and does not move a median, and the question being asked is what the
// cluster does typically, not what it does on average including the once it
// stalled.
package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"sort"

	"dist-mq/e2e-tests/bench"
)

func main() {
	dir := flag.String("dir", "", "directory of per-run JSON files (required)")
	flag.Parse()

	if *dir == "" {
		log.Fatal("-dir is required")
	}

	results, err := load(*dir)
	if err != nil {
		log.Fatalf("loading results: %v", err)
	}
	if len(results) == 0 {
		log.Fatalf("no result files in %s", *dir)
	}

	byArm := map[string][]bench.Result{}
	var order []string
	for _, r := range results {
		if _, seen := byArm[r.Arm]; !seen {
			order = append(order, r.Arm)
		}
		byArm[r.Arm] = append(byArm[r.Arm], r)
	}
	sort.Slice(order, func(i, j int) bool {
		return byArm[order[i]][0].ClusterSize < byArm[order[j]][0].ClusterSize
	})

	first := results[0]
	fmt.Println("# dist-mq benchmark")
	fmt.Println()
	fmt.Printf("%d queue(s), %d publisher(s) and %d subscriber(s) per queue, %d-byte payloads, "+
		"%.0f msg/s offered, %.0fs per run (%.0fs warm-up discarded).\n",
		first.Queues, first.PublishersPerQueue, first.SubscribersPerQueue, first.PayloadBytes,
		first.OfferedRate, first.DurationSec, first.WarmupSec)
	fmt.Println()
	fmt.Println("Medians across repetitions.")
	fmt.Println()
	fmt.Println("| arm | nodes | reps | accepted/s | delivered/s | p50 ms | p99 ms | max ms | ambiguous | rejected |")
	fmt.Println("| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |")

	for _, arm := range order {
		runs := byArm[arm]
		fmt.Printf("| %s | %d | %d | %.0f | %.0f | %.2f | %.2f | %.2f | %d | %d |\n",
			arm,
			runs[0].ClusterSize,
			len(runs),
			medianOf(runs, func(r bench.Result) float64 { return r.AcceptedPerSec }),
			medianOf(runs, func(r bench.Result) float64 { return r.DeliveredPerSec }),
			medianOf(runs, func(r bench.Result) float64 { return r.Latency.P50 }),
			medianOf(runs, func(r bench.Result) float64 { return r.Latency.P99 }),
			medianOf(runs, func(r bench.Result) float64 { return r.Latency.Max }),
			sumOf(runs, func(r bench.Result) int { return r.Ambiguous }),
			sumOf(runs, func(r bench.Result) int { return r.Rejected }),
		)
	}

	fmt.Println()
	fmt.Println("Ambiguous and rejected are totals across every repetition, not medians: " +
		"they are counts of writes the cluster could not cleanly accept, and a run " +
		"that produced them is worth seeing even when most runs did not.")
	fmt.Println()
	fmt.Println("`delivered/s` well below `accepted/s` means the cluster took the writes " +
		"faster than it could push them to subscribers, so the queue grew for the whole " +
		"run. Both rates are measured over the same window, and deliveries for messages " +
		"accepted near the end of it have not landed yet, so a small shortfall is " +
		"expected — a large one is backlog.")
}

func load(dir string) ([]bench.Result, error) {
	paths, err := filepath.Glob(filepath.Join(dir, "*.json"))
	if err != nil {
		return nil, err
	}
	sort.Strings(paths)

	var out []bench.Result
	for _, p := range paths {
		raw, err := os.ReadFile(p)
		if err != nil {
			return nil, err
		}
		var r bench.Result
		if err := json.Unmarshal(raw, &r); err != nil {
			return nil, fmt.Errorf("%s: %w", p, err)
		}
		out = append(out, r)
	}
	return out, nil
}

func medianOf(runs []bench.Result, pick func(bench.Result) float64) float64 {
	v := make([]float64, 0, len(runs))
	for _, r := range runs {
		v = append(v, pick(r))
	}
	sort.Float64s(v)
	if len(v) == 0 {
		return 0
	}
	mid := len(v) / 2
	if len(v)%2 == 1 {
		return v[mid]
	}
	return (v[mid-1] + v[mid]) / 2
}

func sumOf(runs []bench.Result, pick func(bench.Result) int) int {
	total := 0
	for _, r := range runs {
		total += pick(r)
	}
	return total
}
