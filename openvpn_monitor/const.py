import datetime

ALL = "__ALL__"
INF = "inf"

TIMEDELTAS = {
    "15 minutes": datetime.timedelta(minutes=15),
    "30 minutes": datetime.timedelta(minutes=30),
    "1 hour": datetime.timedelta(hours=1),
    "3 hours": datetime.timedelta(hours=3),
    "6 hours": datetime.timedelta(hours=6),
    "12 hours": datetime.timedelta(hours=12),
    "1 day": datetime.timedelta(days=1),
    "1 week": datetime.timedelta(weeks=1),
    "2 weeks": datetime.timedelta(weeks=2),
    "30 days": datetime.timedelta(days=30),
    "90 days": datetime.timedelta(days=90),
    "180 days": datetime.timedelta(days=180),
    "365 days": datetime.timedelta(days=365),
    INF: None,
}

DATA_SIZES = ["B", "KiB", "MiB", "GiB", "TiB", "PiB"]
DATA_SPEEDS = ["B/s", "KiB/s", "MiB/s", "GiB/s", "TiB/s", "PiB/s"]
