import multiprocessing
import sys
import time
from typing import Dict, Any

from openvpn_monitor.monitoring.openvpn import OVPNMonitor
from openvpn_monitor.monitoring.sql import OVPNSessionsWriter, OVPNDataWriter


def monitor(
    *,
    hosts: Dict[str, Dict[str, Any]],
    connection_string: str,
    data_table: str,
    sessions_table: str,
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
        OVPNDataWriter(
            queue=data_queue,
            connection_string=connection_string,
            table=data_table,
        )
    )

    processes.append(
        OVPNSessionsWriter(
            queue=sessions_queue,
            connection_string=connection_string,
            table=sessions_table,
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
                    process.terminate()
                process.join()
            sys.exit(1)
        time.sleep(10)
