import multiprocessing
import os
import sys
import time

import pypika
import yaml

from openvpn_monitor.monitoring.monitor import monitor

from openvpn_monitor.dashboard.dashboard import get_dashboard

SCHEMA = "ovpnmonitor"
DATA_TABLE = "data"
SESSIONS_TABLE = "sessions"


def main():
    with open("/config.yaml") as fd:
        config = yaml.safe_load(fd)
    interval = int(os.environ.get("INTERVAL", "60"))
    timeout = int(os.environ.get("TIMEOUT", "5"))

    data_table = pypika.Table(DATA_TABLE, schema=SCHEMA)
    sessions_table = pypika.Table(SESSIONS_TABLE, schema=SCHEMA)
    mysql_creds = {
        "host": os.environ["MYSQL_HOST"],
        "port": int(os.environ["MYSQL_PORT"]),
        "user": os.environ["MYSQL_USER"],
        "database": None,
    }

    processes = []

    print("Starting monitor", flush=True)
    processes.append(
        multiprocessing.Process(
            target=monitor,
            name="monitor",
            kwargs={
                "hosts": config["hosts"],
                "connection_string": mysql_creds,
                "data_table": data_table,
                "sessions_table": sessions_table,
                "interval": interval,
                "timeout": min(timeout, interval),
            }
        )
    )

    dashboard = get_dashboard(
        mysql_creds=mysql_creds,
        data_table=data_table,
        sessions_table=sessions_table,
    )

    processes.append(
        multiprocessing.Process(
            target=dashboard.run_server,
            name="webserver",
            kwargs={
                "host": "0.0.0.0",
                "port": 8888,
            }
        )
    )

    for process in processes:
        process.start()

    while True:
        if not all([process.is_alive() for process in processes]):
            print("Some processes died", flush=True)
            for process in processes:
                if process.is_alive():
                    process.terminate()
                process.join()
            sys.exit(1)
        time.sleep(10)


if __name__ == "__main__":
    main()
