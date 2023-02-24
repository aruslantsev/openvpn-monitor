import datetime
from typing import List, Optional, Dict

import mysql.connector
import pandas as pd
import pypika
from pypika import Query, Order

from openvpn_monitor.constraints.columns import (
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
        mysql_creds: Dict[str, str],
        table: pypika.Table,
    ):
        self.creds = mysql_creds
        self.table = table

    def __call__(
        self,
        timedelta: Optional[datetime.timedelta] = None
    ) -> List[str]:
        query = Query.from_(self.table).select(HOST).distinct()

        if timedelta is not None:
            min_timestamp = (datetime.datetime.now() - timedelta).timestamp()
            query = query.where(self.table[CONNECTED_AT] >= min_timestamp)

        connection = mysql.connector.connect(**self.creds, database=None)
        cursor = connection.cursor()
        cursor.execute(query.get_sql())
        result = cursor.fetchall()
        result = sorted(row[0] for row in result)

        return result


class OVPNDataReader:
    def __init__(
        self,
        mysql_creds: Dict[str, str],
        table: pypika.Table,
    ):
        self.creds = mysql_creds
        self.table = table
        self.columns = [HOST, TIMESTAMP_START, TIMESTAMP_END, USER, RECEIVED, SENT]

    def __call__(
        self,
        host: Optional[str] = None,
        connected_at_min: Optional[datetime.datetime] = None,
        limit: Optional[int] = None,
    ) -> pd.DataFrame:
        query = Query.from_(self.table).select(*self.columns)

        if host is not None:
            query = query.where(self.table[HOST] == host)
        if connected_at_min is not None:
            query = query.where(self.table[TIMESTAMP_START] >= connected_at_min.timestamp())

        query = query.orderby(TIMESTAMP_START, order=Order.desc)
        if limit is not None:
            query = query.limit(limit)

        connection = mysql.connector.connect(**self.creds, database=None)
        cursor = connection.cursor()
        cursor.execute(query.get_sql())
        result = cursor.fetchall()

        return pd.DataFrame(result, columns=self.columns)


class OVPNSessionsReader:
    def __init__(
        self,
        mysql_creds: Dict[str, str],
        table: pypika.Table,
    ):
        self.creds = mysql_creds
        self.table = table
        self.columns = [HOST, USER, IP, INTERNAL_IP, RECEIVED, SENT, CONNECTED_AT, CLOSED_AT]

    def __call__(
        self,
        host: Optional[str] = None,
        connected_at_min: Optional[datetime.datetime] = None,
        limit: Optional[int] = None,
    ):
        query = Query.from_(self.table).select(*self.columns)

        if host is not None:
            query = query.where(self.table[HOST] == host)
        if connected_at_min is not None:
            query = query.where(self.table[TIMESTAMP_START] >= connected_at_min.timestamp())

        query = query.orderby(CONNECTED_AT, order=Order.desc)
        if limit is not None:
            query = query.limit(limit)

        connection = mysql.connector.connect(**self.creds, database=None)
        cursor = connection.cursor()
        cursor.execute(query.get_sql())
        result = cursor.fetchall()

        return pd.DataFrame(result, columns=self.columns)
