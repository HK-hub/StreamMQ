#!/usr/bin/env python3
# Copyright 2026 StreamMQ Contributors (https://github.com/HK-hub/StreamMQ)
#
# Licensed under the MIT License.
"""StreamMQ 基准回归门禁。

读取 JMH 产出的 JSON 结果（默认 streammq-benchmark/target/jmh-*.json），
与仓库内置基线 benchmark-baseline.json 比对：任一基准相对基线回退超过阈值
（吞吐下降 / 时延上升）即以非零退出码失败，从而阻断引入性能回归的变更。

JMH JSON 由 `ResultFormatType.JSON` 产出，顶层为结果对象数组，每个对象含：
    benchmark, mode, params{...}, primaryMetric{ score, scoreUnits, ... }

复合键 = benchmark | mode | 排序后的 params，用于精确匹配同一基准的同一参数组合。

方向判定（按 scoreUnits）：
    * 含 "/s"（如 "ops/s"）  -> 越大越好（吞吐），当前值低于基线 *(1-阈值) 判回归
    * 其它时间单位（s/ms/us）-> 越小越好（时延），当前值高于基线 *(1+阈值) 判回归

基线引导（bootstrap）：
    * 基线文件不存在时，把本次结果写入基线并正常退出（不比较），便于首次建立基线；
    * --update 时强制用本次结果刷新基线（配合 CI 的 baseline_update 输入，由维护者确认后提交）。
"""

import argparse
import glob
import json
import os
import sys


def _make_key(entry):
    benchmark = entry.get("benchmark", "")
    mode = entry.get("mode", "")
    params = entry.get("params", {}) or {}
    pstr = ",".join(f"{k}={params[k]}" for k in sorted(params))
    return f"{benchmark}|{mode}|{pstr}"


def _is_higher_better(units):
    return (units or "").lower().endswith("/s")


def _load_current(paths):
    results = {}
    for path in paths:
        with open(path, encoding="utf-8") as fh:
            data = json.load(fh)
        entries = data if isinstance(data, list) else data.get("benchmarks", [data])
        for entry in entries:
            primary = entry.get("primaryMetric", {}) or {}
            score = primary.get("score")
            if score is None:
                continue
            key = _make_key(entry)
            results[key] = {
                "benchmark": entry.get("benchmark"),
                "mode": entry.get("mode"),
                "params": entry.get("params", {}) or {},
                "score": score,
                "units": primary.get("scoreUnits"),
            }
    return results


def _human(key):
    benchmark, mode, pstr = key.split("|", 2)
    return f"{benchmark} [{mode}] {pstr}" if pstr else f"{benchmark} [{mode}]"


def main():
    parser = argparse.ArgumentParser(description="StreamMQ benchmark regression gate")
    parser.add_argument(
        "--current",
        required=True,
        help="JMH JSON 结果目录或 glob（如 streammq-benchmark/target）",
    )
    parser.add_argument(
        "--baseline",
        required=True,
        help="基线 JSON 路径（如 streammq-benchmark/benchmark-baseline.json）",
    )
    parser.add_argument(
        "--threshold",
        type=float,
        default=0.15,
        help="回退阈值（小数，默认 0.15 = 15%%）",
    )
    parser.add_argument(
        "--update",
        action="store_true",
        help="用本次结果刷新基线（不比较）",
    )
    args = parser.parse_args()

    if os.path.isdir(args.current):
        current_paths = sorted(glob.glob(os.path.join(args.current, "jmh-*.json")))
    else:
        current_paths = sorted(glob.glob(args.current))
    if not current_paths:
        print(f"::error::no JMH json result files found under {args.current}")
        sys.exit(2)

    current = _load_current(current_paths)
    if not current:
        print("::error::no usable benchmark scores parsed from result files")
        sys.exit(2)
    print(f"Loaded {len(current)} benchmark result(s) from {len(current_paths)} file(s).")

    baseline_missing = not os.path.exists(args.baseline)
    if args.update or baseline_missing:
        with open(args.baseline, "w", encoding="utf-8") as fh:
            json.dump(current, fh, indent=2, sort_keys=True)
        reason = "baseline missing -> initialized" if baseline_missing else "update requested"
        print(f"::notice::Baseline {reason}: {len(current)} entries -> {args.baseline}")
        sys.exit(0)

    with open(args.baseline, encoding="utf-8") as fh:
        baseline = json.load(fh)

    regressions = []
    new_count = 0
    for key, cur in sorted(current.items()):
        if key not in baseline:
            new_count += 1
            print(f"[NEW] {_human(key)} = {cur['score']} {cur['units']}")
            continue
        base = baseline[key]
        bscore = base.get("score")
        cscore = cur.get("score")
        if bscore is None or cscore is None or bscore == 0:
            continue
        higher = _is_higher_better(cur["units"])
        change = (cscore - bscore) / bscore
        if higher:
            regressed = change < -args.threshold
        else:
            regressed = change > args.threshold
        arrow = "↓" if change < 0 else "↑"
        print(
            f"[{'REGRESSION' if regressed else 'OK'}] {_human(key)}: "
            f"{bscore} -> {cscore} {cur['units']} ({arrow}{abs(change) * 100:.1f}%)"
        )
        if regressed:
            regressions.append((key, change))

    if regressions:
        detail = "; ".join(
            f"{_human(k)} ({('↓' if c < 0 else '↑')}{abs(c) * 100:.1f}%)" for k, c in regressions
        )
        print(
            f"::error::{len(regressions)} benchmark regression(s) exceeded "
            f"threshold {args.threshold * 100:.0f}%: {detail}"
        )
        sys.exit(1)

    print(
        f"OK: no regressions beyond {args.threshold * 100:.0f}% "
        f"(new benchmarks: {new_count})."
    )
    sys.exit(0)


if __name__ == "__main__":
    main()
