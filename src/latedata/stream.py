"""Structured Streaming queries over the event folder."""
from __future__ import annotations

from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

SCHEMA = "event_id string, site_id int, event_time timestamp, mb double, lag_min double"


def session() -> SparkSession:
    s = (SparkSession.builder.master("local[2]").appName("late-data").config("spark.ui.enabled", "false")
         .config("spark.ui.showConsoleProgress", "false").config("spark.sql.shuffle.partitions", "2")
         .config("spark.sql.session.timeZone", "UTC").getOrCreate())
    s.sparkContext.setLogLevel("ERROR")
    return s


def events(spark, folder: Path):
    return (spark.readStream.schema(SCHEMA).option("maxFilesPerTrigger", 1)
            .json(str(folder)))


def windowed_counts(spark, folder: Path, watermark: str | None = "10 minutes", window: str = "10 minutes"):
    df = events(spark, folder)
    if watermark:
        df = df.withWatermark("event_time", watermark)
    return df.groupBy(F.window("event_time", window).alias("w"), "site_id").agg(
        F.count("*").alias("events"), F.round(F.sum("mb"), 2).alias("mb"))


def state_metrics(query) -> dict:
    p = query.lastProgress or {}
    ops = p.get("stateOperators") or [{}]
    return {"state_rows": ops[0].get("numRowsTotal", 0), "dropped_by_watermark": ops[0].get("numRowsDroppedByWatermark", 0),
            "watermark": (p.get("eventTime") or {}).get("watermark")}
