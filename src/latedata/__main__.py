"""python -m latedata   (about 5 minutes: each scenario streams 90 one-minute files)"""
import logging

import pandas as pd

from latedata.experiments import crash_and_restart, simulate
from latedata.stream import session

logging.getLogger("py4j").setLevel(logging.CRITICAL)
pd.set_option("display.width", 200)


def main() -> None:
    spark = session()
    rows = [simulate(spark, wm, mode) for wm, mode in [(None, "update"), ("10 minutes", "update"),
                                                        ("5 minutes", "append"), ("10 minutes", "append"),
                                                        ("40 minutes", "append")]]
    print("== 90 minutes of events (200 a minute; 5% are 3-9 min late, 1% are 31-45 min late), 10-minute windows")
    print("   compared on windows ending by minute 30, which every setting has closed by the end")
    print(pd.DataFrame(rows).to_string(index=False))
    print("\n== the sink writes, then the job dies before Spark commits the batch; restart from the checkpoint")
    spark.sparkContext.setLogLevel("OFF")  # the crash is deliberate; its stack trace is noise here
    print(pd.DataFrame([crash_and_restart(spark, False), crash_and_restart(spark, True)]).to_string(index=False))


if __name__ == "__main__":
    main()
