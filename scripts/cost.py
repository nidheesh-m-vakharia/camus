#!/usr/bin/env python3
"""Monthly cost of a Camus workload, from ADR 0007.

Prices are R2 Standard as recorded in that ADR on 2026-11-05, and they still
match the Cloudflare pricing page updated 2026-10-30: Class A $4.50 per
million, Class B $0.36 per million, storage $0.015 per GB-month (1e9 bytes),
deletes free, R2 egress free.

The ADR's per-GiB figures are a sustained rate, before the free tier. This
estimate also applies three adjustments. Ingest bytes and puts run only
during the hours data is flowing. Dollars subtract the Standard free tier
(1 million Class A, 10 million Class B, 10 GB-month). Class A includes the
orphan sweep, which lists bundle/ every 5 minutes at one request per 1,000
keys. Compaction is on unless --no-compact is set, matching CAMUS_COMPACT_MS.
The table does not round a class up to the next million requests.

Issue #141 has no committed object-store request counts. Benchmarks/ records
latency and throughput, not puts and gets. `--check` locks the put rate to
the worked points in ADR 0007. Those rounded dollar figures ($0.018, $0.0006,
$0.0015 per GiB) are within 2.5% of the unrounded products below. The ADR's
"$1.6 per hour" does not match the expression written next to it; that
expression is $64.80 per hour, and the ADR now says so.

A month is 30 days. One active upload stream uploads a full 8 MiB object
immediately. At or above 4 MiB it flushes on CAMUS_FLUSH_MS. A smaller prefix
waits for 8 MiB or 30 seconds. Partitions that share a stream share that
object. The 250 ms-or-8 MiB products in ADR 0007 are kept below as the old
timer, and the 2026-11-12 amendment is the rate this script uses.
"""

from __future__ import annotations

import argparse
import json
import math
import sys

MONTH_S = 30 * 24 * 3600
CLASS_A = 4.50 / 1_000_000
CLASS_B = 0.36 / 1_000_000
STORAGE_PER_GB = 0.015
FREE_CLASS_A = 1_000_000.0
FREE_CLASS_B = 10_000_000.0
FREE_GB = 10.0
CAP_MIB = 8.0
COMPACT_MIB = 128.0
SNAP_S = 30.0
SWEEP_S = 300.0
LIST_PAGE = 1000.0
GIB = 1024.0**3
GB = 1_000_000_000.0


def puts_per_second(mib_s: float, streams: float, flush_s: float, cap_mib: float) -> float:
    """Objects per second.

    A stream at or above half an object per flush interval keeps the timer.
    A quieter stream waits for a full object, and never longer than 30s.
    """
    if mib_s <= 0 or streams <= 0 or flush_s <= 0 or cap_mib <= 0:
        return 0.0
    per_stream = mib_s / streams
    filling = per_stream >= (cap_mib / 2.0) / flush_s
    if filling:
        each = max(1.0 / flush_s, per_stream / cap_mib)
    else:
        each = max(per_stream / cap_mib, 1.0 / 30.0)
    return streams * each


def billable(amount: float, allowance: float) -> float:
    return max(0.0, amount - allowance)


def estimate(args: argparse.Namespace) -> dict:
    flush_s = args.flush_ms / 1000.0
    streams = args.streams
    hours_day = min(max(args.active_hours, 0.0), 24.0)
    duty = hours_day / 24.0
    puts_s = puts_per_second(args.mib_s, streams, flush_s, CAP_MIB)
    rejected_s = puts_per_second(args.mib_s, args.partitions, flush_s, CAP_MIB)
    ingest_bytes = args.mib_s * 1024.0 * 1024.0 * MONTH_S * duty
    ingest_puts = puts_s * MONTH_S * duty
    snap_puts = MONTH_S / SNAP_S
    compact_ops = (args.mib_s * duty * MONTH_S) / COMPACT_MIB if args.compact else 0.0
    retained = args.mib_s * 1024.0 * 1024.0 * args.retention_days * 86400.0 * duty
    if args.compact and retained > 0:
        objects = retained / (COMPACT_MIB * 1024.0 * 1024.0)
    else:
        objects = (ingest_puts / MONTH_S) * args.retention_days * 86400.0 if MONTH_S else 0.0
    pages = math.ceil(objects / LIST_PAGE) if objects > 0 else 0
    list_requests = pages * (MONTH_S / SWEEP_S)
    class_a = ingest_puts + snap_puts + compact_ops + list_requests
    class_b = puts_s * args.fanout * args.miss * MONTH_S * duty + compact_ops
    storage_gb = retained / GB
    if args.mode == "wal":
        footprint = args.rf * args.mib_s * 1024.0 * 1024.0 * args.lag_s
        cap = args.brokers * 8.0 * GIB
        disk_bytes = min(footprint, cap)
        follower = max(args.rf - 1, 0) * ingest_bytes
    else:
        disk_bytes = 0.0
        follower = 0.0
    client_egress = ingest_bytes * args.fanout
    upload = ingest_bytes
    net_bytes = ingest_bytes + follower + upload + client_egress
    hours = MONTH_S / 3600.0
    return {
        "puts_per_s": puts_s,
        "rejected_puts_per_s": rejected_s,
        "ingest_puts": ingest_puts,
        "list_requests": list_requests,
        "class_a_requests": class_a,
        "class_b_requests": class_b,
        "class_a_usd": billable(class_a, FREE_CLASS_A) * CLASS_A,
        "class_b_usd": billable(class_b, FREE_CLASS_B) * CLASS_B,
        "storage_gb": storage_gb,
        "storage_usd": billable(storage_gb, FREE_GB) * STORAGE_PER_GB,
        "disk_gib": disk_bytes / GIB,
        "disk_usd": (disk_bytes / GB) * args.disk_per_gb,
        "ingress_gib": ingest_bytes / GIB,
        "follower_gib": follower / GIB,
        "upload_gib": upload / GIB,
        "egress_gib": client_egress / GIB,
        "network_gib": net_bytes / GIB,
        "network_usd": (net_bytes / GIB) * args.net_per_gib,
        "compute_usd": args.brokers * args.hourly * hours,
        "hours": hours,
        "timer_binds": args.mib_s > 0
        and hours_day > 0
        and streams > 0
        and (args.mib_s / streams) >= (CAP_MIB / 2.0) / flush_s
        and (args.mib_s / streams) < (CAP_MIB / flush_s),
        "ingest_gib": ingest_bytes / GIB,
    }


def notes(args: argparse.Namespace, row: dict) -> list[str]:
    out = []
    if row["timer_binds"]:
        out.append(
            "The upload timer binds. Each active stream flushes every "
            f"{args.flush_ms:.0f} ms instead of filling an 8 MiB object, so "
            "Class A per byte stays high. A longer flush interval cuts puts "
            "and, in r2 mode, raises acknowledgement latency."
        )
    if args.streams > 1 and row["timer_binds"]:
        out.append(
            "Each active WAL shard has its own timer. Spreading a small ingest "
            "across more shards multiplies Class A puts. Partitions that share "
            "a shard do not."
        )
    if args.fanout * args.miss >= 2:
        out.append(
            "Sealed reads dominate Class B: fan-out times the cache-miss share, "
            "once per ingest object. Reads served from the write-ahead log are "
            "not in this count. After compaction, a historical read is one GET "
            "per 128 MiB."
        )
    if args.active_hours < 24:
        out.append(
            "MiB/s is the rate while ingest is flowing, "
            f"{args.active_hours:g} hours a day. The other hours add no puts and "
            "no bytes. Snapshots and the orphan sweep still run all month."
        )
    if row["list_requests"] > 0:
        if args.compact:
            shape = "Compaction is on, so the live set is about one key per 128 MiB."
        else:
            shape = "Compaction is off, so one key per flush stays for the retention window."
        out.append(
            "The orphan sweep lists bundle/ every 5 minutes. R2 bills one Class A "
            f"request per 1,000 keys, {row['list_requests']:.0f} list requests this "
            f"month. {shape}"
        )
    out.append(
        "Dollars are after R2 Standard’s free tier: 1 million Class A requests, "
        "10 million Class B requests, and 10 GB-month. Cloudflare also rounds a "
        "class up to the next million requests and storage up to the next GB. "
        "This table does not."
    )
    return out


def parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(description="Estimate a month of Camus on R2.")
    p.add_argument("--check", action="store_true")
    p.add_argument("--mib-s", type=float, default=1.0)
    p.add_argument("--active-hours", type=float, default=8.0)
    p.add_argument("--partitions", type=float, default=1.0)
    p.add_argument("--streams", type=float, default=1.0)
    p.add_argument("--retention-days", type=float, default=7.0)
    p.add_argument("--fanout", type=float, default=1.0)
    p.add_argument("--miss", type=float, default=0.0)
    p.add_argument("--brokers", type=float, default=3.0)
    p.add_argument("--hourly", type=float, default=0.04)
    p.add_argument("--mode", choices=("wal", "r2"), default="wal")
    p.add_argument("--rf", type=float, default=3.0)
    p.add_argument("--lag-s", type=float, default=1.0)
    p.add_argument("--flush-ms", type=float, default=250.0)
    p.add_argument("--disk-per-gb", type=float, default=0.08)
    p.add_argument("--net-per-gib", type=float, default=0.01)
    p.add_argument("--compact", action=argparse.BooleanOptionalAction, default=True)
    return p


def check() -> None:
    flush_s = 0.25
    assert puts_per_second(1, 1, flush_s, 8) == 0.125
    assert puts_per_second(20, 1, flush_s, 8) == 4
    per_gib = 4 * 1024 * 4.50 / 1_000_000
    assert per_gib == 0.018432
    assert abs(per_gib - 0.018) / 0.018 < 0.025
    assert puts_per_second(100, 1, flush_s, 8) == 12.5
    fast = 0.125 * 1024 * 4.50 / 1_000_000
    assert abs(fast - 0.000576) < 1e-12
    assert abs(fast - 0.0006) / 0.0006 < 0.05
    gets = 4 * 1024 * 0.36 / 1_000_000
    assert abs(gets - 0.00147456) < 1e-12
    assert abs(gets - 0.0015) / 0.0015 < 0.025
    assert abs(puts_per_second(1, 1000, flush_s, 8) - (1000.0 / 30.0)) < 1e-9
    assert 4000 * 3600 * 4.50 / 1_000_000 == 64.8
    # Bundled ingest ignores partition count.
    base = parser().parse_args(["--mib-s", "1", "--partitions", "1000"])
    wide = estimate(base)
    assert wide["puts_per_s"] == 0.125
    assert abs(wide["rejected_puts_per_s"] - (1000.0 / 30.0)) < 1e-6
    defaults = parser().parse_args([])
    assert defaults.miss == 0.0
    assert defaults.active_hours == 8.0
    assert defaults.compact is True
    assert defaults.hourly == 0.04
    assert defaults.disk_per_gb == 0.08
    assert defaults.net_per_gib == 0.01
    open_log = parser().parse_args(
        [
            "--mib-s",
            "1",
            "--active-hours",
            "8",
            "--no-compact",
            "--miss",
            "0",
            "--retention-days",
            "7",
        ]
    )
    listed = estimate(open_log)
    objects = 0.125 * (8 / 24) * 7 * 86400
    pages = math.ceil(objects / LIST_PAGE)
    assert listed["list_requests"] == pages * (MONTH_S / SWEEP_S)
    assert abs(listed["ingest_puts"] - 0.125 * MONTH_S * (8 / 24)) < 1e-6
    assert listed["class_b_requests"] == 0
    assert listed["class_a_usd"] == billable(listed["class_a_requests"], FREE_CLASS_A) * CLASS_A
    packed = estimate(
        parser().parse_args(
            ["--mib-s", "1", "--active-hours", "8", "--retention-days", "7"]
        )
    )
    assert packed["list_requests"] < listed["list_requests"]
    assert packed["class_b_usd"] == 0
    print("cost model matches ADR 0007 worked points")


def main() -> None:
    args = parser().parse_args()
    if args.check:
        check()
        return
    row = estimate(args)
    row["notes"] = notes(args, row)
    row["total_usd"] = (
        row["class_a_usd"]
        + row["class_b_usd"]
        + row["storage_usd"]
        + row["disk_usd"]
        + row["network_usd"]
        + row["compute_usd"]
    )
    json.dump(row, sys.stdout, indent=2)
    sys.stdout.write("\n")


if __name__ == "__main__":
    main()
