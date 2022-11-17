import multiprocessing
import syslog
import telnetlib
import time
from typing import List, Dict, Tuple

from openvpn_monitor.monitoring.data import SessionData, SessionBytes


class OVPNMonitor:
    def __init__(
        self,
        *,
        host_alias: str = "VPNServer",
        host: str = "localhost",
        port: int = 7505,
        sessions_queue: multiprocessing.Queue,
        data_queue: multiprocessing.Queue,
        interval: int = 10,
        timeout: int = 5,
    ):
        self.host_alias = host_alias
        self.host = host
        self.port = port
        self.sessions_queue = sessions_queue
        self.data_queue = data_queue
        self.interval = interval
        self.timeout = timeout

    def status(
        self,
    ) -> List[str]:
        try:
            telnet = telnetlib.Telnet(self.host, self.port)
            telnet.write(b"status\n")
            result = b""
            while True:
                chunk = telnet.read_until(b"END", self.timeout)
                if chunk == b"":
                    break
                result += chunk

            telnet.close()

            result = result.decode('utf-8')
            result = result.splitlines()
            status = [s for s in result if s.startswith("CLIENT_LIST")]

            return status
        except EOFError as e:
            syslog.syslog(
                syslog.LOG_ERR,
                (
                    f"{self.host_alias}: Got {e.__repr__()} when tried to fetch data "
                    f"from the monitoring port for {self.host}:{self.port}"
                )
            )
            return []

    def status_parsed(
        self
    ) -> Tuple[int, Dict[str, SessionData]]:
        status = self.status()
        stats = {}
        timestamp = int(time.time())
        for line in status:
            # CLIENT_LIST,Common Name,Real Address,Virtual Address,Virtual IPv6 Address,
            # Bytes Received,Bytes Sent,Connected Since,Connected Since (time_t),Username,
            # Client ID,Peer ID,Data Channel Cipher
            _, user, ip, internal_ip, _, sent, received, connected_at_str, connected_at = (
                line.split(",")[:9]
            )
            sess_id = user + ip + internal_ip + connected_at_str
            sess_stats = SessionData(
                host=self.host_alias,
                user=user,
                ip=ip,
                internal_ip=internal_ip,
                sent=int(sent),
                received=int(received),
                connected_at_str=connected_at_str,
                connected_at=int(connected_at),
                closed_at=None,
            )
            stats[sess_id] = sess_stats

        return timestamp, stats

    def run(
        self,
    ):
        syslog.syslog(syslog.LOG_INFO, f'Started monitoring for host {self.host_alias}...')
        timestamp = int(time.time())
        status = {}

        while True:
            start = time.time()
            timestamp_prev, status_prev = timestamp, status
            timestamp, status = self.status_parsed()
            expired_sessions = [
                sess_id for sess_id in status_prev if sess_id not in status
            ]
            active_sessions = [
                sess_id for sess_id in status_prev if sess_id in status
            ]

            for sess in expired_sessions:
                status_prev[sess].closed_at = timestamp_prev
                self.sessions_queue.put((self.host_alias, status_prev[sess]))

            user_dw_data = {'__ALL__': {'sent': 0, 'received': 0}}
            for sess in active_sessions:
                if status[sess].user not in user_dw_data:
                    user_dw_data[status[sess].user] = {'sent': 0, 'received': 0}
                if sess in status_prev:
                    user_sent = status[sess].sent - status_prev[sess].sent
                    user_received = status[sess].received - status_prev[sess].received
                else:
                    user_sent = status[sess].sent
                    user_received = status[sess].received
                user_dw_data[status[sess].user]['sent'] += user_sent
                user_dw_data[status[sess].user]['received'] += user_received

                user_dw_data['__ALL__']['sent'] += user_sent
                user_dw_data['__ALL__']['received'] += user_received

            if (
                    (user_dw_data['__ALL__']['sent'] != 0)
                    or (user_dw_data['__ALL__']['received'] != 0)
            ):
                self.data_queue.put(
                    SessionBytes(
                        host=self.host_alias,
                        timestamp_start=timestamp_prev,
                        timestamp_end=timestamp,
                        data=user_dw_data,
                    )
                )

            time_wait = self.interval - (time.time() - start)
            if time_wait > 0:
                time.sleep(time_wait)
            else:
                syslog.syslog(syslog.LOG_WARNING, 'Not enough time to collect stats')


class SimpleReader:
    def __init__(
        self,
        queue: multiprocessing.Queue
    ):
        self.queue = queue

    def run(self):
        while True:
            try:
                data = self.queue.get()
                print(int(time.time()), data, flush=True)
            except ValueError:
                print("Error in SimpleReader", flush=True)
