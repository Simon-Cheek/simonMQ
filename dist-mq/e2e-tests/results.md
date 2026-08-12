# dist-mq benchmark

3 queue(s), 1 publisher(s) and 1 subscriber(s) per queue, 64-byte payloads, 600 msg/s offered, 30s per run (5s warm-up discarded).

Medians across repetitions.

| arm | nodes | reps | accepted/s | delivered/s | p50 ms | p99 ms | max ms | ambiguous | rejected |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| 1-node | 1 | 3 | 600 | 593 | 4.57 | 22.79 | 51.71 | 0 | 0 |
| 3-node | 3 | 3 | 600 | 147 | 19.20 | 60.50 | 103.48 | 0 | 0 |
| 5-node | 5 | 3 | 600 | 96 | 28.13 | 76.32 | 126.78 | 0 | 0 |

Ambiguous and rejected are totals across every repetition, not medians: they are counts of writes the cluster could not cleanly accept, and a run that produced them is worth seeing even when most runs did not.

`delivered/s` well below `accepted/s` means the cluster took the writes faster than it could push them to subscribers, so the queue grew for the whole run. Both rates are measured over the same window, and deliveries for messages accepted near the end of it have not landed yet, so a small shortfall is expected — a large one is backlog.