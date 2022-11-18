import datetime

ALL = "__ALL__"

TIMEDELTAS = {
    "15m": datetime.timedelta(minutes=15),
    "30m": datetime.timedelta(minutes=30),
    "1h": datetime.timedelta(hours=1),
    "3h": datetime.timedelta(hours=3),
    "6h": datetime.timedelta(hours=6),
    "12h": datetime.timedelta(hours=12),
    "1d": datetime.timedelta(days=1),
}

DATA_SIZES = ["B", "KiB", "MiB", "GiB", "TiB", "PiB"]
DATA_SPEEDS = ["B/s", "KiB/s", "MiB/s", "GiB/s", "TiB/s", "PiB/s"]
