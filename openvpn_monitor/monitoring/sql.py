import multiprocessing

from sqlalchemy import create_engine
from sqlalchemy.orm import Session

from openvpn_monitor.columns import (
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


class OVPNSessionsWriter(multiprocessing.Process):
    def __init__(self, queue, connection_string, table):
        super().__init__(name="session_writer")
        self.queue = queue
        self.connection_string = connection_string
        self.table = table

        engine = create_engine(self.connection_string, pool_recycle=1800)
        with Session(engine) as session:
            session.execute(
                f'''CREATE TABLE IF NOT EXISTS {self.table}
                    (
                        {HOST} VarChar(255) NOT NULL,
                        {USER} Text, 
                        {IP} Text, 
                        {INTERNAL_IP} Text, 
                        {SENT} Integer, 
                        {RECEIVED} Integer, 
                        {CONNECTED_AT_STR} Text, 
                        {CONNECTED_AT} Integer,
                        {CLOSED_AT} Integer
                    )
                    PARTITION BY KEY ({HOST})'''
            )
            session.commit()

    def run(self):
        engine = create_engine(self.connection_string, pool_recycle=1800)
        while True:
            ovpn_session: SessionData = self.queue.get()
            query = (
                f"""
                INSERT INTO {self.table} 
                    (
                        {HOST},
                        {USER}, 
                        {IP}, 
                        {INTERNAL_IP}, 
                        {SENT}, 
                        {RECEIVED}, 
                        {CONNECTED_AT_STR}, 
                        {CONNECTED_AT}, 
                        {CLOSED_AT}
                    ) 
                    VALUES
                    (
                        "{ovpn_session.host}",
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


class OVPNDataWriter(multiprocessing.Process):
    def __init__(self, queue, connection_string, table):
        super().__init__(name="data_writer")
        self.queue = queue
        self.connection_string = connection_string
        self.table = table

        engine = create_engine(self.connection_string, pool_recycle=1800)
        with Session(engine) as session:
            session.execute(
                f'''CREATE TABLE IF NOT EXISTS {self.table}
                    (
                        {HOST} VarChar(255) NOT NULL,
                        {TIMESTAMP_START} Integer, 
                        {TIMESTAMP_END} Integer, 
                        {USER} Text, 
                        {SENT} Integer, 
                        {RECEIVED} Integer
                    )
                    PARTITION BY KEY ({HOST})'''
            )
            session.commit()

    def run(self):
        engine = create_engine(self.connection_string, pool_recycle=1800)
        while True:
            sessionbytes: SessionBytes = self.queue.get()
            for user in sessionbytes.data:
                query = (
                    f"""
                    INSERT INTO {self.table} 
                        (
                            {HOST},
                            {TIMESTAMP_START}, 
                            {TIMESTAMP_END}, 
                            {USER}, 
                            {SENT}, 
                            {RECEIVED}
                        )
                        VALUES
                        (
                            "{sessionbytes.host}",
                            "{sessionbytes.timestamp_start}", 
                            "{sessionbytes.timestamp_end}", 
                            "{user}", 
                            "{sessionbytes.data[user]['sent']}", 
                            "{sessionbytes.data[user]['received']}"
                        )
                    """
                )
                with Session(engine) as session:
                    session.execute(query)
                    session.commit()
