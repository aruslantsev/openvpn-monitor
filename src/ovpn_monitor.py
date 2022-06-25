import multiprocessing
import os
import syslog
import telnetlib
import time
from dataclasses import dataclass
from typing import Optional

from sqlalchemy import create_engine
from sqlalchemy.orm import Session


@dataclass
class SessionStats:
    user: str
    ip: str
    internal_ip: str
    sent: int
    received: int
    connected_at_str: str
    connected_at: int
    closed_at: Optional[int] = None


class OVPNPoller:
    def __init__(
            self,
            host: str = "localhost",
            port: int = 7505,
            queue: multiprocessing.Queue = None,
    ):
        self.host = host
        self.port = port
        self.queue = queue

    def status(self, timeout: int = 5):
        try:
            telnet = telnetlib.Telnet(self.host, self.port)
            telnet.write(b"status\n")
            result = b""
            while True:
                chunk = telnet.read_until(b"END", timeout)
                if chunk == b"":
                    break
                result += chunk

            telnet.close()

            result = result.decode('utf-8')
            result = result.splitlines()
            status = [s for s in result if s.startswith("CLIENT_LIST")]

            return status
        except EOFError:
            return []

    def status_parsed(self, timeout: int = 5):
        status = self.status(timeout)
        stats = {}
        for line in status:
            _, user, ip, internal_ip, _, sent, received, connected_at_str, connected_at = (
                line.split(",")[:9]
            )
            sess_id = user + ip + internal_ip + connected_at_str
            sess_stats = SessionStats(
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

        return int(time.time()), stats

    def run(self, polling_interval=10):
        syslog.syslog(syslog.LOG_INFO, 'Started poller')
        while True:
            start = time.time()
            timestamp, status = self.status_parsed(timeout=3)
            self.queue.put((timestamp, status))  # set timeout for debug
            time_wait = polling_interval - (time.time() - start)
            if time_wait > 0:
                time.sleep(time_wait)
            else:
                syslog.syslog(syslog.LOG_WARNING, 'Not enough time to collect stats')


class OVPNStatsAggregator:
    def __init__(
            self,
            input_queue: multiprocessing.Queue,
            sessions_queue: multiprocessing.Queue,
            data_queue: multiprocessing.Queue
    ):
        self.input_queue = input_queue
        self.sessions_queue = sessions_queue
        self.data_queue = data_queue

    def run(self):
        syslog.syslog(syslog.LOG_INFO, 'Started stats aggregator')
        timestamp = int(time.time())
        status = {}

        while True:
            if not self.input_queue.empty():
                timestamp_prev, status_prev = timestamp, status
                try:
                    timestamp, status = self.input_queue.get(timeout=1)
                    expired_sessions = [sess_id for sess_id in status_prev if sess_id not in status]
                    active_sessions = [sess_id for sess_id in status_prev if sess_id in status]

                    for sess in expired_sessions:
                        status_prev[sess].closed_at = timestamp_prev
                        self.sessions_queue.put(status_prev[sess])

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
                        self.data_queue.put((timestamp_prev, timestamp, user_dw_data))
                except ValueError:
                    syslog.syslog(syslog.LOG_ERR, 'Error in Aggregator: queue is empty')
                    timestamp, status = timestamp_prev, status_prev
            else:
                time.sleep(1)


class OVPNSessionsWriter:
    def __init__(self, queue, conn_string, table):
        self.queue = queue

        self.conn_string = conn_string
        self.table = table

        engine = create_engine(self.conn_string, pool_recycle=1800)
        with Session(engine) as session:
            session.execute(
                f'''CREATE TABLE IF NOT EXISTS {self.table}
                    (
                        user Text, 
                        ip Text, 
                        internal_ip Text, 
                        sent Integer, 
                        received Integer, 
                        connected_at_str Text, 
                        connected_at Integer,
                        closed_at Integer
                    )'''
            )
            session.commit()

    def run(self):
        engine = create_engine(self.conn_string, pool_recycle=1800)
        while True:
            try:
                ovpn_session: SessionStats = self.queue.get()
                query = (
                    f"""INSERT INTO {self.table} 
                            (
                                user, 
                                ip, 
                                internal_ip, 
                                sent, 
                                received, 
                                connected_at_str, 
                                connected_at, 
                                closed_at
                            ) 
                            VALUES
                            (
                                "{ovpn_session.user}", 
                                "{ovpn_session.ip}", 
                                "{ovpn_session.internal_ip}", 
                                "{ovpn_session.sent}", 
                                "{ovpn_session.received}", 
                                "{ovpn_session.connected_at_str}", 
                                "{ovpn_session.connected_at}", 
                                "{ovpn_session.closed_at}"
                            )
                        """
                )
                with Session(engine) as session:
                    session.execute(query)
                    session.commit()
            except ValueError:
                syslog.syslog(syslog.LOG_ERR, "Error in SessionWriter")


class OVPNDataWriter:
    def __init__(self, queue, conn_string, table):
        self.queue = queue

        self.conn_string = conn_string
        self.table = table

        engine = create_engine(self.conn_string, pool_recycle=1800)
        with Session(engine) as session:
            session.execute(
                f'''CREATE TABLE IF NOT EXISTS {self.table}
                    (
                        timestamp_start Integer, 
                        timestamp_end Integer, 
                        user Text, 
                        sent Integer, 
                        received Integer
                    )'''
            )
            session.commit()

    def run(self):
        engine = create_engine(self.conn_string, pool_recycle=1800)
        while True:
            try:
                timestamp_start, timestamp_end, data = self.queue.get()
                for user in data:
                    query = (
                        f"""INSERT INTO {self.table} 
                                (
                                    timestamp_start, 
                                    timestamp_end, 
                                    user, 
                                    sent, 
                                    received
                                )
                                VALUES
                                (
                                    "{timestamp_start}", 
                                    "{timestamp_end}", 
                                    "{user}", 
                                    "{data[user]['sent']}", 
                                    "{data[user]['received']}"
                                )
                            """
                    )
                    with Session(engine) as session:
                        session.execute(query)
                        session.commit()
            except ValueError:
                syslog.syslog(syslog.LOG_ERR, "Error in DataWriter")


class SimpleReader:
    def __init__(self, queue):
        self.queue = queue

    def run(self):
        while True:
            try:
                data = self.queue.get()
                print(time.time(), data, flush=True)
            except ValueError:
                print("Error in SimpleReader", flush=True)


def poller():
    connection_string = os.environ['CONNECTION_STRING']

    host = os.environ['HOST'] if 'HOST' in os.environ else 'localhost'
    port = int(os.environ['PORT']) if 'PORT' in os.environ else 7505

    sessions_table = "sessions"
    data_table = "data"

    syslog.openlog(ident="ovpn-monitor-collector", facility=syslog.LOG_DAEMON)

    q_poller_aggregator = multiprocessing.Queue(100)
    q_aggregator_sessions = multiprocessing.Queue(100)
    q_aggregator_data = multiprocessing.Queue(100)

    vpnpoller = OVPNPoller(host, port, queue=q_poller_aggregator)

    aggregator = OVPNStatsAggregator(
        input_queue=q_poller_aggregator,
        sessions_queue=q_aggregator_sessions,
        data_queue=q_aggregator_data
    )

    sessions_writer = OVPNSessionsWriter(
        queue=q_aggregator_sessions, conn_string=connection_string, table=sessions_table)
    data_writer = OVPNDataWriter(
        queue=q_aggregator_data, conn_string=connection_string, table=data_table)

    p_poller = multiprocessing.Process(target=vpnpoller.run, args=(60,))
    p_aggregator = multiprocessing.Process(target=aggregator.run)
    p_sessions_writer = multiprocessing.Process(target=sessions_writer.run)
    p_data_writer = multiprocessing.Process(target=data_writer.run)

    processes = [p_poller, p_aggregator, p_sessions_writer, p_data_writer]

    for process in processes:
        process.start()

    while True:
        if not all([process.is_alive() for process in processes]):
            syslog.syslog(syslog.LOG_ERR, "Poller: some processes died")
            for process in processes:
                if process.is_alive():
                    process.terminate()
                process.join()
            raise Exception("Poller: some processes died")
        time.sleep(10)


def run_poller():
    while True:
        try:
            poller()
        except Exception as e:
            print(e, flush=True)
            time.sleep(10)


if __name__ == "__main__":
    run_poller()
