import datetime

import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy.orm import Session

from openvpn_monitor.columns import (
    USER,
    IP,
    INTERNAL_IP,
    RECEIVED,
    SENT,
    CONNECTED_AT,
    CLOSED_AT,
    TIMESTAMP_START,
    TIMESTAMP_END,
)
from openvpn_monitor.const import DATA_SPEEDS, DATA_SIZES


class OVPNSessionsReader:
    def __init__(self, conn_string, table):
        self.conn_string = conn_string
        self.table = table

        self.engine = create_engine(self.conn_string, pool_recycle=1800)

    def __call__(self, connected_at_min=None, limit=None):
        # engine = create_engine(self.conn_string, pool_recycle=1800)
        query = (
            f"""SELECT 
                    {USER}, 
                    {IP}, 
                    {INTERNAL_IP}, 
                    {RECEIVED}, 
                    {SENT}, 
                    {CONNECTED_AT}, 
                    {CLOSED_AT}
                FROM {self.table} 
            """
        )
        if connected_at_min is not None:
            query += (
                f"""WHERE {CONNECTED_AT} >= {connected_at_min}
                 """
            )
        query += f"""ORDER BY {CONNECTED_AT} DESC 
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
            columns=[USER, IP, INTERNAL_IP, RECEIVED, SENT, CONNECTED_AT, CLOSED_AT,]
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
                    {TIMESTAMP_START}, 
                    {TIMESTAMP_END}, 
                    {USER}, 
                    {RECEIVED},
                    {SENT} 
                FROM {self.table} 
            """
        )
        if connected_at_min is not None:
            query += (
                f"""WHERE {TIMESTAMP_START} >= {connected_at_min} 
                 """
            )
        query += f"""ORDER BY {TIMESTAMP_START} DESC 
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
            columns=[TIMESTAMP_START, TIMESTAMP_END, USER, RECEIVED, SENT,]
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

    sizes = DATA_SIZES
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
    sizes = DATA_SPEEDS
    denominator = 1024

    i = 0
    while x / denominator >= 1:
        if i < len(sizes) - 1:
            i += 1
            x /= denominator
        else:
            break

    return f"{x:.2f} {sizes[i]}"
