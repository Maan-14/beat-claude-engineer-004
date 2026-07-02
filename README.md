# Real-Time Analytics Pipeline - Beat Claude (engineer-004)

System-design submission for **Single Grain's Beat Claude** hiring challenge, role target **Senior Engineer** ([challenge brief](https://github.com/ericosiu/beat-claude/tree/main/challenges/engineer-004)).

**The problem:** rebuild a martech analytics pipeline that ingests ~50M events/day, drop event-to-dashboard latency from 15–30 min to **under 5 s**, survive **10× traffic spikes with zero data loss**, and do it multi-tenant on AWS under a $50K/mo ceiling - without changing the SDK on 500+ customer sites.

**The core idea:** put a **durable stream (Kinesis) between collection and processing**, then consume it with an autoscaling stateful processor (Managed Flink). This converts *data loss under load* into a *bounded, recoverable latency excursion* - a tradeoff proven, not asserted, by the benchmark in this repo.

## Headline result

A dependency-free discrete-event benchmark runs the current-style design and the proposed design against the **same** 10× / 5 s burst:

| metric | Baseline (current-style) | Proposed |
|---|---|---|
| loss | **24.3%** | **0%** |
| latency p50 | 15.5 s | 800 ms |
| within <5 s SLA (steady state) | 12.0% | **97.9%** |
| worst case during burst | 36.7 s | 6.6 s |
| p99 recovery after spike | - | ~7 s |

*All numbers `[Observed – local simulation]` (seed=42), not production infrastructure. It demonstrates the mechanism and quantifies the tradeoff.*

## Run the benchmark

```bash
cd benchmark
python3 pipeline_bench.py       # Python 3 stdlib only, no dependencies
```

Writes `bench_result.json` and `bench_trace.csv`. Edit the parameter block at the top of `pipeline_bench.py` (spike length/multiplier, buffer sizes, autoscale lag) and re-run to explore other scenarios.

## Repository layout

```
submission/
  engineer-004-submission.md      the written answer (source)
  engineer-004-submission.docx    formatted deliverable (Word)
  engineer-004-submission.pdf     formatted deliverable (PDF)
benchmark/
  pipeline_bench.py               the operating artifact (runnable)
  bench_result.json               headline metrics
  bench_trace.csv                 per-second trace (arrivals, backlog, p99)
diagram/
  engineer-004-architecture.png   reference architecture (raster)
  engineer-004-architecture.svg   reference architecture (vector source)
```

## What stays human

Migration go/no-go per tenant · GDPR/CCPA deletion-completeness sign-off (legal) · the ClickHouse self-host vs. managed decision · any personalization rule acting on a freshly stitched identity before it's confidence-checked.

---

*Prepared by Armughan - Senior AI/ML Engineer.
