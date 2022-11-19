import multiprocessing
import os
import sys
import time

import yaml

from openvpn_monitor.monitoring.monitor import run_monitor


def main():
    with open("/config.yaml") as fd:
        config = yaml.safe_load(fd)
    connection_string = os.environ["CONNECTION_STRING"]
    interval = int(os.environ.get("INTERVAL", "60"))
    timeout = int(os.environ.get("TIMEOUT", "5"))

    sessions_table = "sessions"
    data_table = "data"

    processes = []
    kwargs = dict(
        hosts=config["hosts"],
        connection_string=connection_string,
        data_table=data_table,
        sessions_table=sessions_table,
        interval=interval,
        timeout=min(timeout, interval)
    )

    print("Starting monitor", flush=True)
    processes.append(
        multiprocessing.Process(target=run_monitor, name="monitor", kwargs=kwargs)
    )

    # server_p = multiprocessing.Process(target=app.run_server,
    #                                    kwargs={'host': "0.0.0.0", 'port': 8888})

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
