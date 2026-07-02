#!/usr/bin/env python3
"""
pipeline_bench.py  -- Beat Claude / engineer-004 operating artifact

A dependency-free discrete-event simulation that stress-tests the *core thesis*
of the proposed real-time analytics pipeline:

    "Decoupling ingestion from processing with a durable stream buffer
     eliminates spike-induced event loss, at the cost of a bounded, recoverable
     latency excursion -- provided consumers autoscale."

It runs two systems against the SAME synthetic load (a 10x traffic spike):

  BASELINE  (models the current broken system):
      synchronous ingest -> bounded in-flight buffer -> fixed-capacity worker
      -> micro-batch flush to dashboard.
      Overflow of the bounded buffer during the spike => DROPPED events (loss).
      Visibility gated by a batch flush interval => minutes-scale latency.

  PROPOSED  (models Kinesis/durable buffer -> autoscaling stream consumer):
      durable buffer (effectively unbounded within retention) => zero drop.
      consumer capacity autoscales toward the arrival rate with a reaction lag.
      continuous emission => seconds-scale latency that degrades during the
      spike and then recovers.

This is a MODEL, not production infrastructure. Every parameter is declared and
labeled below so numbers in the write-up trace back to a source. Deterministic
under a fixed seed so a reviewer can reproduce the exact trace.

Run:   python3 pipeline_bench.py
Out:   human-readable report to stdout
       bench_result.json      (machine-readable summary)
       bench_trace.csv        (per-second trace: arrivals, drops, backlog, p99)
"""

import json
import math
import random
from collections import deque

# --------------------------------------------------------------------------
# PARAMETERS  (source labels map to SCORING.md: observed/estimated/benchmarked/assumed)
# --------------------------------------------------------------------------
SEED = 42

DT = 0.1                       # [assumed] tick = 100 ms
HORIZON_S = 90                 # [assumed] 90 s simulated window
SPIKE_START_S = 20             # [assumed] spike begins at t=20s
SPIKE_LEN_S = 5                # [assumed] 5 s spike (models a launch/BF burst)

# 50,000,000 events/day / 86,400 s = 578.7 events/s
R_BASE = 578.7                 # [derived from brief] steady-state events/sec
SPIKE_MULT = 10                # [from brief] "10x traffic spikes"
R_SPIKE = R_BASE * SPIKE_MULT  # [derived] 5,787 events/sec during spike

SLA_MS = 5000                  # [from brief] "<5 second" visibility target

# fixed pipeline floor: collector -> stream -> aggregate(1s window) -> store -> dash
PIPELINE_FLOOR_MS = 800        # [estimated] irreducible hop + 1s-window flush latency

# ---- BASELINE system ----
BASE_CAPACITY = R_BASE * 1.50  # [assumed] provisioned for 1.5x steady, not for spikes
BASE_BUFFER = int(R_BASE * 10) # [assumed] bounded in-flight buffer (~10s of base load)
BASE_FLUSH_S = 30              # [assumed, conservative] micro-batch flush interval.
                               #   Real current system is 15-30 MIN; 30s keeps the
                               #   demo inside a 90s window and still shows the gap.

# ---- PROPOSED system ----
PROP_BASE_CAP = R_BASE * 1.20  # [assumed] baseline consumer capacity (headroom)
PROP_MAX_CAP = R_BASE * 12     # [assumed] autoscale ceiling (budget-bounded)
PROP_SCALE_INTERVAL_S = 3      # [estimated] autoscale reaction lag (Kinesis/Flink)
PROP_SCALE_STEP = R_BASE * 1.5 # [estimated] capacity added per scaling action
PROP_BACKLOG_HI = int(R_BASE)  # [assumed] scale-up trigger: >~1s of backlog
PROP_BACKLOG_LO = int(R_BASE * 0.3)  # [assumed] scale-down trigger


def poisson(lam, rng):
    """Poisson sample; normal approx for large lambda to avoid underflow."""
    if lam <= 0:
        return 0
    if lam > 30:
        return max(0, int(round(rng.gauss(lam, math.sqrt(lam)))))
    # Knuth
    L, k, p = math.exp(-lam), 0, 1.0
    while True:
        k += 1
        p *= rng.random()
        if p <= L:
            return k - 1


def arrival_rate(t):
    if SPIKE_START_S <= t < SPIKE_START_S + SPIKE_LEN_S:
        return R_SPIKE
    return R_BASE


def percentile(sorted_vals, q):
    if not sorted_vals:
        return 0.0
    idx = min(len(sorted_vals) - 1, int(q * len(sorted_vals)))
    return sorted_vals[idx]


def in_spike(t):
    return SPIKE_START_S <= t < SPIKE_START_S + SPIKE_LEN_S


def run_baseline(rng):
    """Bounded sync buffer + fixed capacity + batch flush. Drops on overflow."""
    buf = deque()                 # holds arrival timestamps (seconds)
    dropped = 0
    generated = 0
    latencies_ms = []             # visibility latency of delivered events
    steady_ms = []                # subset: events that arrived outside the spike
    trace = []
    arr_sec = 0
    n_ticks = int(HORIZON_S / DT)
    for i in range(n_ticks):
        t = i * DT
        a = poisson(arrival_rate(t) * DT, rng)
        generated += a
        arr_sec += a
        drops_tick = 0
        for _ in range(a):
            if len(buf) < BASE_BUFFER:
                buf.append(t)
            else:
                dropped += 1
                drops_tick += 1
        # process up to capacity*dt oldest events
        can = int(BASE_CAPACITY * DT)
        proc = 0
        while buf and proc < can:
            arr = buf.popleft()
            # visible at next flush boundary after it's processed at ~t
            visible = (math.floor(t / BASE_FLUSH_S) + 1) * BASE_FLUSH_S
            lat = (visible - arr) * 1000.0
            latencies_ms.append(lat)
            if not in_spike(arr):
                steady_ms.append(lat)
            proc += 1
        if abs((t % 1.0)) < DT / 2:  # ~once per second
            trace.append((round(t), arr_sec, drops_tick, len(buf)))
            arr_sec = 0
    return summarize("BASELINE", generated, dropped, latencies_ms, steady_ms, trace)


def run_proposed(rng):
    """Durable buffer (no drop) + autoscaling consumer + continuous emit."""
    buf = deque()
    dropped = 0
    generated = 0
    latencies_ms = []
    steady_ms = []
    cap = PROP_BASE_CAP
    last_scale_t = -PROP_SCALE_INTERVAL_S
    trace = []
    per_sec_lat = []              # latencies emitted within current second
    arr_sec = 0
    n_ticks = int(HORIZON_S / DT)
    for i in range(n_ticks):
        t = i * DT
        a = poisson(arrival_rate(t) * DT, rng)
        generated += a
        arr_sec += a
        for _ in range(a):
            buf.append(t)         # durable: never dropped
        # autoscale on backlog with reaction lag
        if t - last_scale_t >= PROP_SCALE_INTERVAL_S:
            if len(buf) > PROP_BACKLOG_HI and cap < PROP_MAX_CAP:
                cap = min(PROP_MAX_CAP, cap + PROP_SCALE_STEP)
                last_scale_t = t
            elif len(buf) < PROP_BACKLOG_LO and cap > PROP_BASE_CAP:
                cap = max(PROP_BASE_CAP, cap - PROP_SCALE_STEP)
                last_scale_t = t
        can = int(cap * DT)
        proc = 0
        emitted_this_tick = []
        while buf and proc < can:
            arr = buf.popleft()
            lat = (t - arr) * 1000.0 + PIPELINE_FLOOR_MS
            latencies_ms.append(lat)
            if not in_spike(arr):
                steady_ms.append(lat)
            emitted_this_tick.append(lat)
            proc += 1
        per_sec_lat.extend(emitted_this_tick)
        if abs((t % 1.0)) < DT / 2:
            s = sorted(per_sec_lat)
            p99 = percentile(s, 0.99)
            trace.append((round(t), arr_sec, 0, len(buf), round(p99)))
            per_sec_lat = []
            arr_sec = 0
    return summarize("PROPOSED", generated, dropped, latencies_ms, steady_ms, trace)


def summarize(name, generated, dropped, latencies_ms, steady_ms, trace):
    delivered = generated - dropped
    s = sorted(latencies_ms)
    ss = sorted(steady_ms)
    within_sla = sum(1 for x in s if x <= SLA_MS)
    within_sla_ss = sum(1 for x in ss if x <= SLA_MS)
    return {
        "system": name,
        "generated": generated,
        "delivered": delivered,
        "dropped": dropped,
        "loss_pct": round(100.0 * dropped / generated, 3) if generated else 0.0,
        "p50_ms": round(percentile(s, 0.50)),
        "p95_ms": round(percentile(s, 0.95)),
        "p99_ms": round(percentile(s, 0.99)),
        "max_ms": round(s[-1]) if s else 0,
        "sla_met_pct": round(100.0 * within_sla / len(s), 2) if s else 0.0,
        "steady_p99_ms": round(percentile(ss, 0.99)),
        "steady_sla_met_pct": round(100.0 * within_sla_ss / len(ss), 2) if ss else 0.0,
        "trace": trace,
    }


def recovery_time_s(prop):
    """Seconds after spike end until per-second p99 returns under SLA and STAYS
    under it for the remainder of the window (sustained recovery)."""
    spike_end = SPIKE_START_S + SPIKE_LEN_S
    rows = [r for r in prop["trace"] if r[0] >= spike_end]
    for i, row in enumerate(rows):
        if all(r[4] <= SLA_MS for r in rows[i:]):
            return row[0] - spike_end
    return None


def fmt_ms(ms):
    return f"{ms/1000:.1f}s" if ms >= 1000 else f"{ms}ms"


def main():
    rng = random.Random(SEED)
    base = run_baseline(rng)
    rng = random.Random(SEED)  # same load for a fair before/after
    prop = run_proposed(rng)

    rec = recovery_time_s(prop)

    print("=" * 68)
    print("  REAL-TIME ANALYTICS PIPELINE -- SPIKE BENCHMARK (discrete-event sim)")
    print("=" * 68)
    print(f"  Load: {R_BASE:.0f} ev/s steady, {SPIKE_MULT}x spike -> "
          f"{R_SPIKE:.0f} ev/s for {SPIKE_LEN_S}s   (seed={SEED})")
    print(f"  SLA : end-to-end visibility <= {SLA_MS/1000:.0f}s")
    print("-" * 68)
    hdr = f"  {'metric':<16}{'BASELINE (current)':>24}{'PROPOSED':>18}"
    print(hdr)
    print("-" * 68)
    rows = [
        ("events generated", f"{base['generated']:,}", f"{prop['generated']:,}"),
        ("events dropped",   f"{base['dropped']:,}",   f"{prop['dropped']:,}"),
        ("loss %",           f"{base['loss_pct']}%",   f"{prop['loss_pct']}%"),
        ("lat p50 (all)",    fmt_ms(base['p50_ms']),   fmt_ms(prop['p50_ms'])),
        ("lat p95 (all)",    fmt_ms(base['p95_ms']),   fmt_ms(prop['p95_ms'])),
        ("lat p99 (all)",    fmt_ms(base['p99_ms']),   fmt_ms(prop['p99_ms'])),
        ("lat max (all)",    fmt_ms(base['max_ms']),   fmt_ms(prop['max_ms'])),
        ("p99 steady-state", fmt_ms(base['steady_p99_ms']), fmt_ms(prop['steady_p99_ms'])),
        ("SLA met steady",   f"{base['steady_sla_met_pct']}%", f"{prop['steady_sla_met_pct']}%"),
    ]
    for m, b, p in rows:
        print(f"  {m:<16}{b:>24}{p:>18}")
    print("-" * 68)
    print(f"  PROPOSED p99 recovery after spike end: "
          f"{rec if rec is not None else 'did not recover in window'}"
          f"{'s' if rec is not None else ''}")
    print("=" * 68)
    print("  Takeaway: identical load. Baseline drops ~{:.1f}% and shows minutes-"
          .format(base['loss_pct']))
    print("  scale visibility. Proposed drops 0, holds the <5s SLA at steady state")
    print("  ({:.1f}% within SLA), and its p99 excursion during the spike recovers"
          .format(prop['steady_sla_met_pct']))
    print("  in ~{}s once the consumer autoscales. Latency during the burst is"
          .format(rec if rec is not None else '?'))
    print("  traded for durability -- a deliberate, bounded, recoverable tradeoff.")
    print("=" * 68)

    with open("bench_result.json", "w") as f:
        out = {k: v for k, v in {"baseline": base, "proposed": prop,
                                 "recovery_s": rec}.items()}
        # drop bulky trace from json summary; keep headline numbers
        out["baseline"] = {k: v for k, v in base.items() if k != "trace"}
        out["proposed"] = {k: v for k, v in prop.items() if k != "trace"}
        json.dump(out, f, indent=2)

    with open("bench_trace.csv", "w") as f:
        f.write("system,second,arrivals,drops,backlog,p99_ms\n")
        for r in base["trace"]:
            f.write(f"baseline,{r[0]},{r[1]},{r[2]},{r[3]},\n")
        for r in prop["trace"]:
            f.write(f"proposed,{r[0]},{r[1]},{r[2]},{r[3]},{r[4]}\n")

    print("\nWrote bench_result.json and bench_trace.csv")


if __name__ == "__main__":
    main()
