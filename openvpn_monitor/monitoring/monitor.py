import multiprocessing
import sys
import syslog
import time
from typing import Dict, Any

from openvpn_monitor.monitoring.ovpn import OVPNMonitor
from openvpn_monitor.monitoring.sql import OVPNSessionsWriter, OVPNDataWriter


class Monitor:
    def __init__(
        self,
        *,
        hosts: Dict[str, Dict[str, Any]],
        connection_string: str,
        data_table: str,
        sessions_table: str,
        interval: int = 10,
        timeout: int = 5,
    ):
        self.processes = []
        self.sessions_queue = multiprocessing.Queue(maxsize=len(hosts) * 2)
        self.data_queue = multiprocessing.Queue(maxsize=len(hosts) * 2)
        for host, conf in hosts.items():
            self.processes.append(
                OVPNMonitor(
                    host_alias=host,
                    host=conf['host'],
                    port=conf['monitoring_port'],
                    sessions_queue=self.sessions_queue,
                    data_queue=self.data_queue,
                    interval=interval,
                    timeout=timeout,
                )
            )

        self.processes.append(
            OVPNDataWriter(
                queue=self.data_queue,
                connection_string=connection_string,
                table=data_table,
            )
        )

        self.processes.append(
            OVPNSessionsWriter(
                queue=self.sessions_queue,
                connection_string=connection_string,
                table=sessions_table,
            )
        )
        syslog.openlog(ident="ovpn-monitor", facility=syslog.LOG_DAEMON)

    def run(self):
        for process in self.processes:
            process.start()

        while True:
            if not all([process.is_alive() for process in self.processes]):
                syslog.syslog(syslog.LOG_ERR, "Monitor: some processes died")
                for process in self.processes:
                    if process.is_alive():
                        process.terminate()
                    process.join()
                sys.exit(1)
            time.sleep(10)
