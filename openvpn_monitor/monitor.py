from ovpn_monitor import run_poller
from webserver import app
import multiprocessing


def main():
    poller_p = multiprocessing.Process(target=run_poller)
    server_p = multiprocessing.Process(target=app.run_server,
                                       kwargs={'host': "0.0.0.0", 'port': 8888})
    processes = [poller_p, server_p]
    for process in processes:
        process.start()


if __name__ == "__main__":
    main()
