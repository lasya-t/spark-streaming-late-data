from datetime import datetime

import pytest

from latedata.experiments import crash_and_restart, simulate, true_counts
from latedata.feed import events_for_minute
from latedata.stream import session


@pytest.fixture(scope="module")
def spark():
    return session()


def test_feed_has_late_and_very_late_events():
    ev = [e for m in range(30) for e in events_for_minute(m)]
    lags = [e["lag_min"] for e in ev]
    assert any(3 <= x <= 9 for x in lags) and any(x > 30 for x in lags)


def test_truth_buckets_by_event_time():
    c = true_counts(10)
    assert all(k[0].minute % 10 == 0 and k[0].second == 0 for k in c)
    assert sum(c.values()) == 2000


def test_no_watermark_loses_nothing_but_keeps_state(spark):
    r = simulate(spark, None, "update", minutes=45)
    assert r["lost_%"] == 0 and r["state_rows_at_end"] > 20


def test_watermark_bounds_state_and_drops_very_late_events(spark):
    r = simulate(spark, "10 minutes", "append", minutes=45)
    assert r["state_rows_at_end"] <= 10 and 0 < r["lost_%"] < 5
    assert r["minutes_from_window_end_to_first_output"] == 10.0


def test_upsert_sink_survives_a_replayed_batch(spark):
    app, ups = crash_and_restart(spark, False, minutes=32), crash_and_restart(spark, True, minutes=32)
    assert app["crashed_and_restarted"] and ups["crashed_and_restarted"]
    assert app["duplicate_rows_from_replayed_batch"] > 0
    assert ups["rows_in_sink"] < app["rows_in_sink"]
