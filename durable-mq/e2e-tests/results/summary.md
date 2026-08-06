# Benchmark summary

Load generator host(s): simon

Offered load: 225 msg/s. Baseline arm: `durable-off`.
Throughput and latency are medians across repetitions; brackets are the interquartile range.

| Arm | Reps | Accepted msg/s (IQR) | vs durable-off | p50 ms (IQR) | p99 ms (IQR) | Failed | Saturated |
|---|---|---|---|---|---|---|---|
| `push-mq` | 10 | 225 [225–225] | 1.00x | 0.81 [0.80–0.81] | 1.73 [1.63–1.77] | 0 | no |
| `durable-off` | 10 | 225 [225–225] | — | 0.83 [0.80–0.83] | 1.69 [1.59–1.75] | 0 | no |
| `durable-nosync` | 10 | 225 [225–225] | 1.00x | 0.83 [0.82–0.83] | 1.64 [1.56–1.76] | 0 | no |
| `durable-sync` | 10 | 225 [225–225] | 1.00x | 3.14 [3.13–3.15] | 14.22 [13.60–14.78] | 0 | no |

## Delivery

| Arm | Accepted | Delivered | Ratio |
|---|---|---|---|
| `push-mq` | 56250 | 56280 | 1.001 |
| `durable-off` | 56250 | 56280 | 1.001 |
| `durable-nosync` | 56250 | 56280 | 1.001 |
| `durable-sync` | 56250 | 56280 | 1.001 |

Both counts cover the measurement window only. A ratio slightly below 1 is expected — publishes are acknowledged before delivery happens, so whatever is in flight when the run stops is never counted. A ratio far below 1 means the delivery path could not keep up with the publish path, and any latency figure from that arm is a queueing delay rather than a service time.
