import multiprocessing
import os
import syslog
import time

from openvpn_monitor.utils.monitoring import (
    OVPNPoller,
    OVPNStatsAggregator,
    OVPNSessionsWriter,
    OVPNDataWriter,
)


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
