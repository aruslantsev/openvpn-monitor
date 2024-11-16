import multiprocessing
import telnetlib
import time
from typing import List, Dict, Tuple

from openvpn_monitor.constraints.columns import RECEIVED, SENT
from openvpn_monitor.constraints.const import ALL
from openvpn_monitor.monitoring.data import SessionData, SessionBytes


class OVPNMonitor(multiprocessing.Process):
    """
    This class must have only one instance on OpenVPN server.
    It collects data and puts it into queues
    """
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
        super().__init__(name=f"monitor:{host_alias}")
        self.host_alias = host_alias
        self.host = host
        self.port = port
        self.sessions_queue = sessions_queue
        self.data_queue = data_queue
        self.interval = interval
        self.timeout = timeout

    def status_raw(
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
            print(
                f"{self.host_alias}: Got {e.__repr__()} when tried to fetch data "
                f"from the monitoring port for {self.host}:{self.port}",
                flush=True
            )
            return []

    def status_parsed(
        self
    ) -> Tuple[int, Dict[str, SessionData]]:
        status = self.status_raw()
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
        print(f'Started monitoring for host {self.host_alias}', flush=True)
        timestamp = int(time.time())
        status = {}

        while True:
            start = time.time()
            timestamp_prev, status_prev = timestamp, status
            timestamp, status = self.status_parsed()
            # Closed sessions
            closed_sessions = [
                sess_id for sess_id in status_prev if sess_id not in status
            ]
            # Active sessions
            active_sessions = [sess_id for sess_id in status]

            for sess in closed_sessions:
                status_prev[sess].closed_at = timestamp_prev
                self.sessions_queue.put(
                    status_prev[sess],  # OpenVPN returns total values
                    block=True,  # Wait for the free slot
                )

            user_dw_data = {ALL: {SENT: 0, RECEIVED: 0}}
            for sess in active_sessions:
                # Each session should appear only once in server logs, but who knows...
                if status[sess].user not in user_dw_data:
                    user_dw_data[status[sess].user] = {SENT: 0, RECEIVED: 0}
                if sess in status_prev:  # old but still active session
                    user_sent = status[sess].sent - status_prev[sess].sent
                    user_received = status[sess].received - status_prev[sess].received
                else:  # new session
                    user_sent = status[sess].sent
                    user_received = status[sess].received
                # Again, should appear only once. This is user stats only for current server
                user_dw_data[status[sess].user][SENT] += user_sent
                user_dw_data[status[sess].user][RECEIVED] += user_received

                # Overall data transfer stats for current server
                user_dw_data[ALL][SENT] += user_sent
                user_dw_data[ALL][RECEIVED] += user_received

            if (
                len(user_dw_data) > 1
            ) or (
                    (user_dw_data[ALL][SENT] != 0)
                    or (user_dw_data[ALL][RECEIVED] != 0)
            ):  # not only ALL keyword or any amount of data
                self.data_queue.put(
                    SessionBytes(
                        host=self.host_alias,
                        timestamp_start=timestamp_prev,
                        timestamp_end=timestamp,
                        data=user_dw_data,
                    ),
                    block=True  # Wait for the free slot
                )

            time_wait = self.interval - (time.time() - start)
            if time_wait > 0:
                time.sleep(time_wait)
            else:
                print("Not enough time to collect stats", flush=True)


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
