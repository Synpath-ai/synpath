# Faster historical book reconstruction

The historical replay example now validates level dictionaries through the
containing `HistoricalOrderBook`, instead of invoking `OrderLevel`'s Python
constructor separately for every level of every reconstructed state. Validation,
Decimal arithmetic, sorting, output models, timestamps and metadata are preserved.
This is a known batching technique, not a new order-book algorithm.

A cProfile capture of 100 levels per side attributed 1.49 s to rebuilding levels
versus 0.013 s to sorting; repeated model validation dominated reconstruction.
The experiment therefore targets model construction, not the sorted book structure.

## Results

Three alternating pairs on an AMD Ryzen AI 7 350, Python 3.13.5, Pydantic 2.14.0,
pinned to CPU 0, one worker. Synthetic full-depth books; exact same seeded inputs
per pair. Median candidate/control time ratios (full min–max):

| Levels per side | Reconstruct states | Reconstruct and JSON encode |
|---|---|---|
| 10 | 0.805 (0.778–0.826) | 0.890 (0.882–0.891) |
| 100 | 0.768 (0.762–0.771) | 0.855 (0.847–0.866) |
| 1000 | 0.855 (0.832–0.864) | 0.904 (0.890–0.913) |

At 100 levels per side this is 23% less reconstruction time and 15% less time
including JSON encoding. These are client reconstruction results, not download
or hosted-service speedups. The Texas Senate dataset itself was not available
locally and was not benchmarked. Saved real API responses can be supplied below.

Each pass includes response-model validation, exact delta application, full
materialization after each change and, for export, the existing model_dump plus
json.dumps pipeline. Network, disk writes and initial snapshot export are excluded.
Warmup precedes timing; fresh processes for each arm; normal GC is enabled.
Depths 10/100 use 1024 changes and three passes; depth 1000 uses 256 changes and
two passes. Seeds 1729, 2738, 3747; sizes start at 10000, then randomized sides
and prices receive alternating +/-1 updates. These distributions are assumptions.

Memory tradeoff: nested validation briefly keeps the level dictionaries alongside
the resulting models, adding O(depth) temporary storage. It does not retain a
history of previous books or share mutable level models across emitted states.
Five differential tests cover empty through 1000-level books, scale-equivalent
prices, deletion/reinsertion, metadata, JSON output equality and output independence.
The existing historical gap/version/range tests pass.

The machine uses the performance platform profile, powersave CPU governor and
battery power (user confirms performance mode). A/A runs and raw paired data are
included; timings remain specific to this machine and these synthetic workloads.
No assertion about tail-latency improvement or actual market download time.

## Reproduce

From the repo root, install the project and pytest, then freeze the old example:

```sh
git show 53af00c49b184d8ae5c3a4e0755fc98b26c25694:examples/track_historical_book.py > /tmp/history-control.py
python benchmarks/compare_history.py --control /tmp/history-control.py --candidate examples/track_historical_book.py --output /tmp/history-ab
python benchmarks/compare_history.py --control /tmp/history-control.py --candidate /tmp/history-control.py --output /tmp/history-aa --depths 100
pytest tests/test_history_tracker.py tests/test_history_types.py -q
python benchmarks/history_replay.py --response recorded-range.json --export
python benchmarks/history_replay.py --depth 100 --latency --export
python benchmarks/history_replay.py --depth 100 --changes 128 --repeats 1 --memory
```

Raw CSV/JSONL trials, frozen source hashes, machine metadata and dependency versions
are in `results/history-*` and `results/environment.txt`. Ratios are computed per
pair, then summarized by median and min/max; a ratio of arm means is not used.
The unchanged original implementation serves as the correctness oracle.

Deferred: changing sorting (too little profile cost), caching mutable level models
(would change output independence), skipping validation (unnecessary), Rust-based
historical replay (not justified by this profile), and the separate Rust decoder
allocation improvement (does not help this historical reconstruction path).

The 100-level A/A paired ratios were 1.010 (0.997–1.024) for reconstruction and
1.015 (0.996–1.052) including encoding, smaller than every A/B improvement at
that depth. A separate tracemalloc diagnostic with 128 changes measured peak
Python memory at 546,845 bytes before and 583,925 after (+37,080 bytes, about 7%).
That diagnostic also enabled latency instrumentation; its timing values are not
used for the speed claim. The output size was identical in every export pair.
See `results/history-ratios.svg` and `results/history-profile.txt`.
