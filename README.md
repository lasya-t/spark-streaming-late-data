# spark-streaming-late-data

Spark Structured Streaming over cell-site usage events that **arrive late**: phones buffer events while
they have no signal and send them when they reconnect. The repo measures what watermarks and output modes
actually trade off (completeness, state size, how long until a number appears), and what happens when the
job dies halfway through writing a batch.

## Setup

- **Feed** (`feed.py`): one JSON file per minute, 200 events each, over 5 cell sites. 94% of events are on
  time, 5% are 3–9 minutes late, and 1% are 31–45 minutes late. Files are written to a temp name and
  renamed, so the stream never reads a half-written file.
- **Query**: 10-minute event-time windows per site, counting events. The run drops one file and processes
  it, 90 times.
- **Truth**: every event bucketed by its real event time. Results are compared on windows ending by minute
  30, which every setting has finished with by the end of the run.

## Results (`PYTHONPATH=src python -m latedata`)

| Watermark | Output mode | Rows written | Events lost | State rows at the end | When a window's number appears |
|---|---|---|---|---|---|
| none | update | 875 | **0%** | 65, and growing | updates from the start, forever |
| 10 min | update | 725 | 1.99% | **10** | updates during the window |
| 5 min | append | 45 | 2.55% | 5 | **5 min** after the window ends |
| 10 min | append | 40 | 1.99% | 10 | 10 min after |
| 40 min | append | 39 | **0.23%** | 25 | 40 min after |

- **No watermark loses nothing and never forgets.** Spark has to keep every window open in case a late
  event turns up, so state grows with every window and never shrinks. On a real stream that runs out of
  memory eventually.
- **A watermark is a promise about lateness.** At 10 minutes, the events more than 10 minutes late (the 1%
  that were 31–45 minutes late, plus a few of the 9-minute ones) are dropped: 1.99% of events. In
  exchange, state stays at 10 rows.
- **Append mode writes each window once, and the watermark sets the wait.** The delay is exactly the
  watermark (5, 10, 40 minutes). A 40-minute watermark catches almost everything (0.23% lost) but answers
  40 minutes later. Update mode answers immediately with partial numbers and corrects them as late events
  arrive, which is why it writes about 18× as many rows.
- **Choose the watermark from the data's lateness, not by default.** Here the late tail goes to 45 minutes,
  so no setting below 45 can be lossless.

## Crashing after the sink writes

The `foreachBatch` sink writes a batch to SQLite, then the job dies before Spark records the batch as
done. On restart from the checkpoint, Spark runs that batch again, which is correct at-least-once behaviour.

| Sink | Rows in the sink | Duplicate rows from the replayed batch | Events in closed windows |
|---|---|---|---|
| append | 335 | **9** | 5,996 |
| upsert keyed by (window, site) | **25** | 0 | 5,996 |

Both arrive at the right totals if a reader takes the latest row per window. But the append sink holds
every intermediate update plus the replayed duplicates, so anyone who sums its rows over-counts. Making
the write idempotent, by upserting on the window key, gives exactly one row per window and site however
many times a batch is replayed. That's how "exactly once" is achieved in practice: the stream replays, and
the sink is built so replays don't matter.

## Run

```bash
pip install -r requirements.txt     # needs Java 17+
PYTHONPATH=src python -m latedata   # ~5 minutes
pytest -q
```
