"""Compare frozen historical replay implementations in alternating order.

python benchmarks/compare_history.py --control /tmp/control.py \
  --candidate examples/track_historical_book.py --output benchmarks/results/history-ab
Use the control path for both arms to measure A/A variation.
"""
import argparse
import csv
import hashlib
import json
import os
from pathlib import Path
import subprocess
import sys

ROOT = Path(__file__).resolve().parents[1]


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--control", type=Path, required=True)
    parser.add_argument("--candidate", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--depths", type=int, nargs="+", default=[10, 100, 1000])
    parser.add_argument("--rounds", type=int, default=3)
    args = parser.parse_args()
    os.sched_setaffinity(0, {0})
    args.output.parent.mkdir(parents=True, exist_ok=True)
    metadata = {arm: {"path": str(getattr(args, arm).resolve()),
                      "sha256": hashlib.sha256(getattr(args, arm).read_bytes()).hexdigest()}
                for arm in ("control", "candidate")}
    metadata.update(python=sys.version, affinity=[0], cpu=subprocess.check_output(["lscpu"], text=True),
                    dependencies=subprocess.check_output([sys.executable, "-m", "pip", "freeze"], text=True)
                    if subprocess.call([sys.executable, "-c", "import pip"], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL) == 0 else "see pyproject.toml and environment.txt")
    Path(str(args.output) + ".metadata.json").write_text(json.dumps(metadata, indent=2) + "\n")
    with Path(str(args.output) + ".jsonl").open("x") as raw, Path(str(args.output) + ".csv").open("x", newline="") as out:
        writer = csv.DictWriter(out, fieldnames=["cell", "round", "arm", "value"])
        writer.writeheader()
        for round_number in range(args.rounds):
            arms = ["control", "candidate"] if round_number % 2 == 0 else ["candidate", "control"]
            for depth in args.depths:
                for export in (False, True):
                    for arm in arms:
                        command = [sys.executable, str(ROOT / "benchmarks/history_replay.py"), "--replay-source",
                                   str(getattr(args, arm).resolve()), "--depth", str(depth), "--seed",
                                   str(1729 + round_number*1009), "--changes", "256" if depth == 1000 else "1024",
                                   "--repeats", "2" if depth == 1000 else "3"]
                        if export:
                            command.append("--export")
                        result = json.loads(subprocess.check_output(command, text=True, cwd=ROOT))
                        result.update(cell=f"history/{'export' if export else 'materialize'}:{depth}",
                                      round=round_number, arm=arm, command=command)
                        raw.write(json.dumps(result) + "\n")
                        raw.flush()
                        writer.writerow(dict(cell=result["cell"], round=round_number, arm=arm, value=result["ns_per_state"]))
                        out.flush()
                print(f"round {round_number+1}: depth {depth}", flush=True)


if __name__ == "__main__":
    main()
