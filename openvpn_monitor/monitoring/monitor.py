import multiprocessing
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
        self.monitors = []
        self.sessions_queue = multiprocessing.Queue(maxsize=len(hosts) * 2)
        self.data_queue = multiprocessing.Queue(maxsize=len(hosts) * 2)
        for host, conf in hosts.items():
            self.monitors.append(
                OVPNMonitor(
                    host_alias=host,
                    host=conf['host'],
                    port=conf['port'],
                    sessions_queue=self.sessions_queue,
                    data_queue=self.data_queue,
                    interval=interval,
                    timeout=timeout,
                )
            )

        self.data_writer = OVPNDataWriter(
            queue=self.data_queue,
            connection_string=connection_string,
            table=data_table,
        )

        self.sessions_writer = OVPNSessionsWriter(
            queue=self.sessions_queue,
            connection_string=connection_string,
            table=sessions_table,
        )
        syslog.openlog(ident="ovpn-monitor", facility=syslog.LOG_DAEMON)

    def run(self):
        processes = []
        for monitor in self.monitors:
            processes.append(
                multiprocessing.Process(
                    target=monitor.run,
                    name=monitor.host_alias,
                )
            )

        processes.append(
            multiprocessing.Process(
                target=self.data_writer.run,
                name="data_writer"
            )
        )

        processes.append(
            multiprocessing.Process(
                target=self.sessions_writer.run,
                name="sessions_writer"
            )
        )

        for process in processes:
            process.start()

        while True:
            if not all([process.is_alive() for process in processes]):
                syslog.syslog(syslog.LOG_ERR, "Monitor: some processes died")
                for process in processes:
                    if process.is_alive():
                        process.terminate()
                    process.join()
                raise Exception("Monitor: some processes died")
            time.sleep(10)
