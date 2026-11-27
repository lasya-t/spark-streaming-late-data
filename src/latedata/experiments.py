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


def crash_and_restart(spark, upsert: bool, crash_at_minute: int = 25, minutes: int = 40) -> dict:
    """foreachBatch writes to SQLite, then the job dies before Spark records the batch as done.
    On restart Spark runs that batch again. An append sink counts it twice; an upsert keyed by
    (window, site) just overwrites the same rows."""
    import sqlite3

    root = Path(tempfile.mkdtemp())
    src, db = root / "in", root / "sink.db"
    con = sqlite3.connect(db)
    con.execute("CREATE TABLE out (w_start TEXT, site_id INT, events INT, batch_id INT"
                + (", PRIMARY KEY (w_start, site_id))" if upsert else ")"))
    con.commit()
    con.close()
    state = {"minute": 0, "crashed": False}

    def sink(batch_df, batch_id):
        rows = [(str(r["w"]["start"]), r["site_id"], r["events"], batch_id) for r in batch_df.collect()]
        c = sqlite3.connect(db)
        sql = ("INSERT INTO out VALUES (?,?,?,?) ON CONFLICT(w_start, site_id) DO UPDATE SET "
               "events = excluded.events, batch_id = excluded.batch_id") if upsert else "INSERT INTO out VALUES (?,?,?,?)"
        c.executemany(sql, rows)
        c.commit()
        c.close()
        if state["minute"] == crash_at_minute and not state["crashed"]:
            state["crashed"] = True
            raise RuntimeError("worker lost after writing, before the batch was committed")

    def start():
        return (windowed_counts(spark, src, "10 minutes").writeStream.outputMode("update").foreachBatch(sink)
                .option("checkpointLocation", str(root / "ck")).start())

    drop(src, 0, events_for_minute(0))
    q = start()
    q.processAllAvailable()
    for m in range(1, minutes):
        state["minute"] = m
        drop(src, m, events_for_minute(m))
        try:
            q.processAllAvailable()
        except Exception:  # noqa: BLE001  the query died; start it again from the checkpoint
            q = start()
            q.processAllAvailable()
    q.stop()
    c = sqlite3.connect(db)
    if upsert:
        total = c.execute("SELECT sum(events) FROM out WHERE w_start < '2025-03-01 08:30:00'").fetchone()[0]
    else:  # an append sink stores every update; the latest per window is what a reader would use
        total = c.execute("""SELECT sum(events) FROM (SELECT w_start, site_id, events, batch_id,
                             row_number() OVER (PARTITION BY w_start, site_id ORDER BY batch_id DESC) AS rn FROM out)
                             WHERE rn = 1 AND w_start < '2025-03-01 08:30:00'""").fetchone()[0]
        dup = c.execute("SELECT count(*) - count(DISTINCT w_start || site_id || batch_id) FROM out").fetchone()[0]
    rows = c.execute("SELECT count(*) FROM out").fetchone()[0]
    c.close()
    shutil.rmtree(root)
    truth = sum(v for k, v in true_counts(minutes).items() if k[0] + timedelta(minutes=10) <= T0 + timedelta(minutes=30))
    out = {"sink": "upsert by (window, site)" if upsert else "append", "crashed_and_restarted": state["crashed"],
           "rows_in_sink": rows, "events_in_closed_windows": total, "events_that_happened": truth}
    if not upsert:
        out["duplicate_rows_from_replayed_batch"] = dup
    return out
