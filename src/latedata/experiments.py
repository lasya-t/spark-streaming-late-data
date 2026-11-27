"""Run the stream minute by minute and measure what watermarks and output modes do."""
from __future__ import annotations

import shutil
import tempfile
from collections import Counter
from datetime import datetime, timedelta
from pathlib import Path

from latedata.feed import T0, drop, events_for_minute
from latedata.stream import windowed_counts

MINUTES = 90
COMPARE_UNTIL = 30  # windows ending by minute 30: closed under every watermark tried, by minute 90


def true_counts(minutes: int = MINUTES) -> Counter:
    """Every event, bucketed by its 10-minute event-time window: what a perfect stream would report."""
    c = Counter()
    for m in range(minutes):
        for e in events_for_minute(m):
            t = datetime.fromisoformat(e["event_time"])
            start = t.replace(minute=t.minute - t.minute % 10, second=0)
            c[(start, e["site_id"])] += 1
    return c


def simulate(spark, watermark: str | None, mode: str, minutes: int = MINUTES) -> dict:
    root = Path(tempfile.mkdtemp())
    src = root / "in"
    emitted = []  # (minute it came out, window start, site, events)
    clock = {"minute": 0}

    def sink(batch_df, batch_id):
        for r in batch_df.collect():
            emitted.append((clock["minute"], r["w"]["start"], r["site_id"], r["events"]))

    drop(src, 0, events_for_minute(0))
    q = (windowed_counts(spark, src, watermark).writeStream.outputMode(mode).foreachBatch(sink)
         .option("checkpointLocation", str(root / "ck")).trigger(processingTime="0 seconds").start())
    q.processAllAvailable()
    progress, emit_minute = [], {}
    for m in range(1, minutes):
        clock["minute"] = m
        drop(src, m, events_for_minute(m))
        q.processAllAvailable()
        p = q.lastProgress
        ops = (p.get("stateOperators") or [{}])[0]
        progress.append({"minute": m, "state_rows": ops.get("numRowsTotal", 0)})
    q.stop()
    shutil.rmtree(root)
    # final count per window = the last value emitted for it (update) or the only one (append)
    final = {}
    first_seen = {}
    for b, start, site, n in emitted:
        final[(start, site)] = n
        first_seen.setdefault((start, site), b)
    truth = true_counts(minutes)
    closed = [k for k in truth if k[0] + timedelta(minutes=10) <= T0 + timedelta(minutes=COMPARE_UNTIL)]
    counted = sum(final.get(k, 0) for k in closed)
    actual = sum(truth[k] for k in closed)
    lat = [first_seen[k] - (k[0] - T0).total_seconds() / 60 - 10 for k in closed if k in first_seen]
    return {"watermark": watermark or "none", "mode": mode, "rows_emitted": len(emitted),
            "events_counted_in_closed_windows": counted, "events_that_happened": actual,
            "lost_%": round(100 * (actual - counted) / actual, 2),
            "state_rows_at_end": progress[-1]["state_rows"], "max_state_rows": max(p["state_rows"] for p in progress),
            "minutes_from_window_end_to_first_output": round(sum(lat) / len(lat), 1) if lat else None}
