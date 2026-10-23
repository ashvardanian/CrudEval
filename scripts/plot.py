# /// script
# requires-python = ">=3.11"
# dependencies = ["matplotlib>=3.9"]
# ///
"""Plot CrudEval JSON reports as SVG, without mixing workloads or configurations."""

import argparse
import json
from pathlib import Path

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt

THEME = {
    "field": "#C44536",
    "ink": "#FFE6A7",
    "frame": "#E5E5E5",
    "font": ["Poppins", "Helvetica", "Arial", "sans-serif"],
}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("reports", type=Path)
    parser.add_argument("--output", type=Path)
    args = parser.parse_args()
    paths = (
        sorted(args.reports.glob("*.json")) if args.reports.is_dir() else [args.reports]
    )
    rows = []
    for path in paths:
        report = json.loads(path.read_text())
        if report.get("schema_version") not in (1, 2):
            raise ValueError(f"Unsupported report schema: {path}")
        version = report["schema_version"]
        config = report["workload"]
        label = f"schema {version} · {report['config']['backend']} · {config['data_model']} · {config['records'][0]:,} records · {config['threads'][0]} threads · {path.stem.rsplit('-', 1)[-1][:6]}"
        rows.extend(
            (label, f"schema {version} / {config['data_model']}", phase)
            for phase in report["phases"]
            if phase["status"] == "completed"
        )
    if not rows:
        parser.error("No completed phases found")
    output = (
        args.output
        or (args.reports if args.reports.is_dir() else args.reports.parent) / "plots"
    )
    output.mkdir(parents=True, exist_ok=True)
    plt.rcParams.update(
        {
            "figure.facecolor": THEME["field"],
            "axes.facecolor": THEME["field"],
            "axes.edgecolor": THEME["frame"],
            "axes.labelcolor": THEME["ink"],
            "axes.titlecolor": THEME["ink"],
            "text.color": THEME["ink"],
            "xtick.color": THEME["ink"],
            "ytick.color": THEME["ink"],
            "grid.color": THEME["ink"],
            "font.family": THEME["font"],
            "svg.fonttype": "none",
        }
    )
    metrics = [
        ("throughput", "Successful entries / second", lambda p: p["throughput"]),
        (
            "latency",
            "Largest operation p99 (µs)",
            lambda p: max(
                (v["p99_ns"] / 1000 for v in p["latency"].values()), default=0
            ),
        ),
        (
            "memory",
            "Peak client RSS (MB)",
            lambda p: p["client_usage"]["rss_max_bytes"] / 2**20,
        ),
        ("disk", "Database size (MB)", lambda p: p["disk_bytes"] / 2**20),
    ]
    workloads = list(
        dict.fromkeys((model, phase["workload"]) for _, model, phase in rows)
    )
    heights = [
        max(
            2.6,
            0.32
            * sum(model == m and phase["workload"] == w for _, model, phase in rows)
            + 0.8,
        )
        for m, w in workloads
    ]
    for name, ylabel, value in metrics:
        figure, axes = plt.subplots(
            len(workloads),
            1,
            figsize=(12, sum(heights)),
            gridspec_kw={"height_ratios": heights},
            squeeze=False,
        )
        for axis, (model, workload) in zip(axes[:, 0], workloads):
            selected = [
                (label, value(phase))
                for label, row_model, phase in rows
                if row_model == model and phase["workload"] == workload
            ]
            labels, values = zip(*selected)
            axis.barh(labels, values, color=THEME["ink"])
            axis.set_title(f"{model} / {workload}", loc="left")
            axis.set_xlabel(ylabel)
            axis.grid(axis="x", alpha=0.2)
            axis.set_axisbelow(True)
        figure.tight_layout()
        figure.savefig(output / f"{name}.svg", format="svg", bbox_inches="tight")
        plt.close(figure)
    print(output)


if __name__ == "__main__":
    main()
