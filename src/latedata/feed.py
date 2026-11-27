"""Cell-site usage events arriving in small files, with some events showing up late.

Phones buffer events when they lose signal and send them when they reconnect, so an event's time (when it
happened) can be minutes behind its arrival. Each `drop()` writes one file the stream will pick up.
"""
from __future__ import annotations

import json
import random
from datetime import datetime, timedelta
from pathlib import Path

T0 = datetime(2025, 3, 1, 8, 0, 0)


def events_for_minute(minute: int, n: int = 200, late_share: float = 0.05, very_late_share: float = 0.01,
                      seed: int = 0) -> list[dict]:
    """Events that arrive during wall-clock minute `minute`. Most happened in that minute; some are
    a few minutes old (late), a few are over half an hour old (very late)."""
    rng = random.Random(seed * 100_000 + minute)
    out = []
    for i in range(n):
        r = rng.random()
        lag = rng.uniform(3, 9) if r < late_share else rng.uniform(31, 45) if r < late_share + very_late_share else 0
        t = T0 + timedelta(minutes=minute, seconds=rng.uniform(0, 59)) - timedelta(minutes=lag)
        out.append({"event_id": f"m{minute:03d}-{i:04d}", "site_id": rng.randint(1, 5),
                    "event_time": t.isoformat(timespec="seconds"), "mb": round(rng.expovariate(1 / 20), 2),
                    "lag_min": round(lag, 1)})
    return out


def drop(folder: Path, minute: int, events: list[dict]) -> Path:
    folder.mkdir(parents=True, exist_ok=True)
    tmp = folder / f".m{minute:04d}.json.tmp"
    tmp.write_text("\n".join(json.dumps(e) for e in events))
    final = folder / f"m{minute:04d}.json"
    tmp.rename(final)  # the stream must never see a half-written file
    return final
