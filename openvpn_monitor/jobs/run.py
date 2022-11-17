import multiprocessing
import os
import sys
import syslog
import time

import yaml

from openvpn_monitor.monitoring.monitor import Monitor


def main():
    config = yaml.safe_dump("/config.yaml")
    connection_string = os.environ["CONNECTION_STRING"]
    interval = int(os.environ.get("INTERVAL", "60"))
    timeout = int(os.environ.get("TIMEOUT", "5"))

    sessions_table = "sessions"
    data_table = "data"

    processes = []
    vpnmonitor = Monitor(
        hosts=config["hosts"],
        connection_string=connection_string,
        data_table=data_table,
        sessions_table=sessions_table,
        interval=interval,
        timeout=min(timeout, interval)
    )

    processes.append(
        multiprocessing.Process(target=vpnmonitor.run, name="monitor")
    )

    # server_p = multiprocessing.Process(target=app.run_server,
    #                                    kwargs={'host': "0.0.0.0", 'port': 8888})

    for process in processes:
        process.start()

    while True:
        if not all([process.is_alive() for process in processes]):
            syslog.syslog(syslog.LOG_ERR, "Some processes died")
            for process in processes:
                if process.is_alive():
                    process.terminate()
                process.join()
            sys.exit(1)
        time.sleep(10)
