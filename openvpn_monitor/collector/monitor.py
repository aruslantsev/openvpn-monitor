import multiprocessing
import sys
import time
from typing import Dict, Any

import pypika

from openvpn_monitor.monitoring.openvpn import OVPNMonitor
from openvpn_monitor.monitoring.sql import sessions_writer, data_writer


def monitor(
    *,
    hosts: Dict[str, Dict[str, Any]],
    mysql_creds: Dict[str, str],
    data_table: pypika.Table,
    sessions_table: pypika.Table,
    interval: int = 10,
    timeout: int = 5,
):
    processes = []
    sessions_queue = multiprocessing.Queue(maxsize=len(hosts) * 2)
    data_queue = multiprocessing.Queue(maxsize=len(hosts) * 2)
    for host, conf in hosts.items():
        processes.append(
            OVPNMonitor(
                host_alias=host,
                host=conf['host'],
                port=conf['monitoring_port'],
                sessions_queue=sessions_queue,
                data_queue=data_queue,
                interval=interval,
                timeout=timeout,
            )
        )

    processes.append(
        multiprocessing.Process(
            name="data_writer",
            target=data_writer,
            kwargs=dict(
                queue=data_queue,
                mysql_creds=mysql_creds,
                data_table=data_table,
            )
        )
    )

    processes.append(
        multiprocessing.Process(
            name="session_writer",
            target=sessions_writer,
            kwargs=dict(
                queue=data_queue,
                mysql_creds=mysql_creds,
                sessions_table=sessions_table,
            )
        )
    )

    print("Starting processes", flush=True)
    for process in processes:
        process.start()

    while True:
        if not all([process.is_alive() for process in processes]):
            print("Monitor: some processes died", flush=True)
            for process in processes:
                if process.is_alive():
                    process.kill()
                process.join()
            sys.exit(1)
        time.sleep(10)
