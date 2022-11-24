import datetime
from typing import List, Optional

import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy.orm import Session

from openvpn_monitor.columns import (
    HOST,
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


class OVPNHostsReader:
    def __init__(
        self,
        conn_string: str,
        table: str,
    ):
        self.conn_string = conn_string
        self.table = table

        self.engine = create_engine(self.conn_string, pool_recycle=1800)

    def __call__(
        self,
        timedelta: Optional[datetime.timedelta] = None
    ) -> List[str]:
        query = f"""SELECT DISTINCT {HOST} FROM {self.table} """

        if timedelta is not None:
            min_timestamp = (datetime.datetime.now() - timedelta).timestamp()
            query += f""" WHERE {CONNECTED_AT} >= {min_timestamp} """

        with Session(self.engine) as session:
            result = session.execute(query).fetchall()

        result = sorted(row[0] for row in result)

        return result


class OVPNDataReader:
    def __init__(
        self,
        conn_string: str,
        table: str,
    ):
        self.conn_string = conn_string
        self.table = table

        self.engine = create_engine(self.conn_string, pool_recycle=1800)

    def __call__(
        self,
        host: Optional[str] = None,
        connected_at_min: Optional[datetime.datetime] = None,
        limit: Optional[int] = None,
    ) -> pd.DataFrame:
        query = (
            f"""SELECT 
                    {HOST},
                    {TIMESTAMP_START}, 
                    {TIMESTAMP_END}, 
                    {USER}, 
                    {RECEIVED},
                    {SENT} 
                FROM {self.table} """
        )
        if host is not None or connected_at_min is not None:
            query += f""" WHERE """
            if host is not None:
                query += f""" {HOST} = '{host}' """
            if host is not None and connected_at_min is not None:
                query += """ AND """
            if connected_at_min is not None:
                query += f""" {TIMESTAMP_START} >= {connected_at_min.timestamp()} """
        query += f""" ORDER BY {TIMESTAMP_START} DESC """
        if limit is not None:
            query += f""" LIMIT {limit} """

        with Session(self.engine) as session:
            result = session.execute(query).fetchall()

        return pd.DataFrame(
            result,
            columns=[HOST, TIMESTAMP_START, TIMESTAMP_END, USER, RECEIVED, SENT, ]
        )


class OVPNSessionsReader:
    def __init__(
        self,
        conn_string: str,
        table: str,
    ):
        self.conn_string = conn_string
        self.table = table

        self.engine = create_engine(self.conn_string, pool_recycle=1800)

    def __call__(
        self,
        host: Optional[str] = None,
        connected_at_min: Optional[datetime.datetime] = None,
        limit: Optional[int] = None,
    ):
        # engine = create_engine(self.conn_string, pool_recycle=1800)
        query = (
            f"""SELECT 
                    {HOST},
                    {USER}, 
                    {IP}, 
                    {INTERNAL_IP}, 
                    {RECEIVED}, 
                    {SENT}, 
                    {CONNECTED_AT}, 
                    {CLOSED_AT}
                FROM {self.table} """
        )
        if host is not None or connected_at_min is not None:
            query += f""" WHERE """
            if host is not None:
                query += f""" {HOST} = '{host}' """
            if host is not None and connected_at_min is not None:
                query += """ AND """
            if connected_at_min is not None:
                query += f""" {TIMESTAMP_START} >= {connected_at_min.timestamp()} """
        query += f""" ORDER BY {CONNECTED_AT} DESC """
        if limit is not None:
            query += f""" LIMIT {limit} """

        with Session(self.engine) as session:
            result = session.execute(query).fetchall()

        return pd.DataFrame(
            result,
            columns=[HOST, USER, IP, INTERNAL_IP, RECEIVED, SENT, CONNECTED_AT, CLOSED_AT, ]
        )
