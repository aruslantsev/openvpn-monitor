import datetime

import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy.orm import Session


class OVPNSessionsReader:
    def __init__(self, conn_string, table):
        self.conn_string = conn_string
        self.table = table

        self.engine = create_engine(self.conn_string, pool_recycle=1800)

    def __call__(self, connected_at_min=None, limit=None):
        # engine = create_engine(self.conn_string, pool_recycle=1800)
        query = (
            f"""SELECT 
                    user, 
                    ip, 
                    internal_ip, 
                    received, 
                    sent, 
                    connected_at, 
                    closed_at
                FROM {self.table} 
            """
        )
        if connected_at_min is not None:
            query += (
                f"""WHERE connected_at >= {connected_at_min}
                 """
            )
        query += """ORDER BY connected_at DESC 
                 """
        if limit is not None:
            query += (
                f"""LIMIT {limit} 
                 """
            )

        with Session(self.engine) as session:
            result = session.execute(query).fetchall()

        return pd.DataFrame(
            result,
            columns=['user', 'ip', 'internal_ip', 'received', 'sent', 'connected_at', 'closed_at']
        )


class OVPNDataReader:
    def __init__(self, conn_string, table):
        self.conn_string = conn_string
        self.table = table

        self.engine = create_engine(self.conn_string, pool_recycle=1800)

    def __call__(self, connected_at_min=None, limit=None):
        # engine = create_engine(self.conn_string, pool_recycle=1800)
        query = (
            f"""SELECT 
                    timestamp_start, 
                    timestamp_end, 
                    user, 
                    received,
                    sent 
                FROM {self.table} 
            """
        )
        if connected_at_min is not None:
            query += (
                f"""WHERE timestamp_start >= {connected_at_min} 
                 """
            )
        query += """ORDER BY timestamp_start DESC 
                 """
        if limit is not None:
            query += (
                f"""LIMIT {limit} 
                 """
            )
        
        with Session(self.engine) as session:
            result = session.execute(query).fetchall()

        return pd.DataFrame(
            result,
            columns=['timestamp_start', 'timestamp_end', 'user', 'received', 'sent', ]
        )


def get_sess_data(data: pd.DataFrame) -> pd.DataFrame:
    data = data[['user', 'ip', 'connected_at', 'closed_at', 'received', 'sent', ]].copy()
    data['connected_at'] = data['connected_at'].map(datetime.datetime.fromtimestamp)
    data['connected_at'] = data['connected_at'].map(lambda x: x.strftime("%Y-%m-%d %H:%M"))
    data['closed_at'] = data['closed_at'].map(datetime.datetime.fromtimestamp)
    data['closed_at'] = data['closed_at'].map(lambda x: x.strftime("%Y-%m-%d %H:%M"))
    data['received'] = data['received'].map(bytes_to_str)
    data['sent'] = data['sent'].map(bytes_to_str)
    return data


def bytes_to_str(x):
    if x is None:
        return x

    sizes = ["B", "KiB", "MiB", "GiB", "TiB", "PiB"]
    denominator = 1024

    i = 0
    while x / denominator >= 1:
        if i < len(sizes) - 1:
            i += 1
            x /= denominator
        else:
            break

    return f"{x:.2f} {sizes[i]}"


def speed_to_str(x):
    if x is None:
        return None
    sizes = ["B/s", "KiB/s", "MiB/s", "GiB/s", "TiB/s", "PiB/s"]
    denominator = 1024

    i = 0
    while x / denominator >= 1:
        if i < len(sizes) - 1:
            i += 1
            x /= denominator
        else:
            break

    return f"{x:.2f} {sizes[i]}"
