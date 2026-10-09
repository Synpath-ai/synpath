"""Time historical reconstruction and optional JSON export, without networking.

Uses the shipped example's replay implementation. Synthetic input is explicitly
not a recorded history of any particular market. --response accepts a saved
order-book/range JSON response and includes model validation in each pass.
"""
from __future__ import annotations

import argparse
import importlib.util
import json
from pathlib import Path
import random
import statistics
import sys
import time
import tracemalloc

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--replay-source", type=Path, help="frozen track_historical_book.py implementation")
    parser.add_argument("--response", type=Path)
    parser.add_argument("--depth", type=int, default=100)
    parser.add_argument("--changes", type=int, default=1024)
    parser.add_argument("--seed", type=int, default=1729)
    parser.add_argument("--repeats", type=int, default=20)
    parser.add_argument("--export", action="store_true", help="include model_dump and JSON encoding; exclude disk I/O")
    parser.add_argument("--memory", action="store_true", help="separate instrumented peak-memory pass")
    parser.add_argument("--latency", action="store_true", help="separate per-state p50/p99 measurements")
    args = parser.parse_args()
    from examples.track_historical_book import _ReplayBook
    from synpath.types import HistoricalOrderBook, OrderBookRangeResponse
    if args.replay_source:
        spec = importlib.util.spec_from_file_location("synpath_benchmark_history", args.replay_source)
        module = importlib.util.module_from_spec(spec)
        sys.modules[spec.name] = module
        spec.loader.exec_module(module)
        _ReplayBook = module._ReplayBook

    rng = random.Random(args.seed)
    if args.response:
        payload = args.response.read_text()
    else:
        initial = HistoricalOrderBook(
            market_id="kalshi:SYNTHETIC", venue="kalshi", as_of_ms=1000,
            depth_scope="full", timestamp=1000,
            bids=[{"price": (4999-i)/10000, "size": 10000} for i in range(args.depth)],
            asks=[{"price": (5001+i)/10000, "size": 10000} for i in range(args.depth)])
        changes = []
        for index in range(args.changes):
            bid = rng.randrange(2) == 0
            level = rng.randrange(args.depth)
            changes.append(dict(kind="delta", observed_at_ms=1001+index,
                                book_side="bid" if bid else "ask",
                                price_exact=f"{(4999-level if bid else 5001+level)/10000:.4f}",
                                quantity_delta_exact="1.00" if index % 2 else "-1.00"))
        payload = json.dumps(dict(metadata={"dataset_version": "synthetic"}, market_id=initial.market_id,
                                  start_ms=1000, end_ms=1001+args.changes, segments=[
                                      dict(kind="data", start_ms=1000, end_ms=1001+args.changes,
                                           initial_book=initial.model_dump(), changes=changes)]))

    timings = []
    latencies = []
    peak = 0
    total_states = total_bytes = 0
    for iteration in range(args.repeats + 1):
        states = size = 0
        if args.memory:
            tracemalloc.start()
        start = time.perf_counter_ns()
        response = OrderBookRangeResponse.model_validate_json(payload)
        for segment in response.segments:
            if segment.kind == "absent":
                continue
            replay = _ReplayBook(segment.initial_book)
            for change in segment.changes:
                state_start = time.perf_counter_ns() if args.latency else 0
                if change.kind == "snapshot":
                    replay.replace(change.book)
                else:
                    replay.delta(change.book_side, change.price_exact, change.quantity_delta_exact)
                book = replay.materialize(change.observed_at_ms, change.venue_timestamp_ms)
                if args.export:
                    size += len(json.dumps(book.model_dump()))
                states += 1
                if args.latency and iteration:
                    latencies.append(time.perf_counter_ns() - state_start)
        elapsed = time.perf_counter_ns() - start
        if iteration:
            timings.append(elapsed)
            total_states += states
            total_bytes += size
        if args.memory:
            peak = max(peak, tracemalloc.get_traced_memory()[1])
            tracemalloc.stop()
    result = dict(workload="saved-response" if args.response else "synthetic",
                          depth=args.depth, seed=args.seed, states=total_states,
                          ns_per_state=sum(timings)/total_states,
                          median_pass_ns=statistics.median(timings), export_bytes=total_bytes,
                          includes="model validation, deltas, full materialization" + (", JSON encoding" if args.export else ""),
                          excludes="network, disk I/O; initial snapshot export")
    if args.memory:
        result["traced_peak_bytes"] = peak
    if args.latency:
        latencies.sort()
        result.update(p50_ns=statistics.median(latencies), p99_ns=latencies[(len(latencies)-1)*99//100])
    print(json.dumps(result))


if __name__ == "__main__":
    main()
