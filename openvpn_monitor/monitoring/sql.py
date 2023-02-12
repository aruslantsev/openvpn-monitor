import multiprocessing
from typing import Dict

import mysql.connector
import pypika
from pypika import Query, Column

from openvpn_monitor.constraints.columns import (
    HOST,
    USER,
    IP,
    INTERNAL_IP,
    RECEIVED,
    SENT,
    CONNECTED_AT_STR,
    CONNECTED_AT,
    CLOSED_AT,
    TIMESTAMP_START,
    TIMESTAMP_END,
)
from openvpn_monitor.monitoring.data import SessionData, SessionBytes


def create_session_table_tddl(table: pypika.Table) -> str:
    tddl = (
        Query.create_table(table)
        .if_not_exists()
        .columns(
            Column(HOST, "VarChar(255)", nullable=False),
            Column(USER, "Text"),
            Column(IP, "Text"),
            Column(INTERNAL_IP, "Text"),
            Column(SENT, "Integer"),
            Column(RECEIVED, "Integer"),
            Column(CONNECTED_AT_STR, "Text"),
            Column(CONNECTED_AT, "Integer"),
            Column(CLOSED_AT, "Integer"),
        )
    ).get_sql()
    tddl += f"""PARTITION BY KEY ({HOST})"""
    return tddl


def create_data_table_tddl(table: pypika.Table) -> str:
    tddl = (
        Query.create_table(table)
        .if_not_exists()
        .columns(
            Column(HOST, "VarChar(255)", nullable=False),
            Column(TIMESTAMP_START, "Integer"),
            Column(TIMESTAMP_END, "Integer"),
            Column(USER, "Text"),
            Column(SENT, "Integer"),
            Column(RECEIVED, "Integer"),
        )
    ).get_sql()
    tddl += f"""PARTITION BY KEY ({HOST})"""
    return tddl


def sessions_writer(
    queue: multiprocessing.Queue,
    mysql_creds: Dict[str, str],
    sessions_table: pypika.Table
):
    connection = mysql.connector.connect(**mysql_creds, database=None)
    cursor = connection.cursor()
    cursor.execute(create_session_table_tddl(sessions_table))
    connection.commit()

    while True:
        ovpn_session: SessionData = queue.get(block=True)
        query = Query.into(sessions_table).insert(
            ovpn_session.host,
            ovpn_session.user,
            ovpn_session.ip,
            ovpn_session.internal_ip,
            ovpn_session.sent,
            ovpn_session.received,
            ovpn_session.connected_at_str,
            ovpn_session.connected_at,
            ovpn_session.closed_at,
        ).get_sql()
        cursor.execute(query)
        connection.commit()


def data_writer(
    queue: multiprocessing.Queue,
    mysql_creds: Dict[str, str],
    data_table: pypika.Table,
):
    connection = mysql.connector.connect(**mysql_creds, database=None)
    cursor = connection.cursor()
    cursor.execute(create_data_table_tddl(data_table))
    connection.commit()

    while True:
        sessionbytes: SessionBytes = queue.get(block=True)
        for user in sessionbytes.data:
            query = Query.into(data_table).insert(
                sessionbytes.host,
                sessionbytes.timestamp_start,
                sessionbytes.timestamp_end,
                user,
                sessionbytes.data[user][SENT],
                sessionbytes.data[user][RECEIVED],
            ).get_sql()
            cursor.execute(query)
            connection.commit()
