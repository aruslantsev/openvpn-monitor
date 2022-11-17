import syslog

from sqlalchemy import create_engine
from sqlalchemy.orm import Session

from openvpn_monitor.monitoring.data import SessionData


class OVPNSessionsWriter:
    def __init__(self, queue, connection_string, table):
        self.queue = queue
        self.connection_string = connection_string
        self.table = table

        engine = create_engine(self.connection_string, pool_recycle=1800)
        with Session(engine) as session:
            session.execute(
                f'''CREATE TABLE IF NOT EXISTS {self.table}
                    (
                        host VarChar(255) NOT NULL,
                        user Text, 
                        ip Text, 
                        internal_ip Text, 
                        sent Integer, 
                        received Integer, 
                        connected_at_str Text, 
                        connected_at Integer,
                        closed_at Integer
                    )
                    PARTITION BY KEY (host)'''
            )
            session.commit()

    def run(self):
        engine = create_engine(self.connection_string, pool_recycle=1800)
        while True:
            try:
                ovpn_session: SessionData = self.queue.get()
                query = (
                    f"""INSERT INTO {self.table} 
                            (
                                host,
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
            except ValueError as e:
                syslog.syslog(syslog.LOG_ERR, f"Error in SessionWriter, {e}")


class OVPNDataWriter:
    def __init__(self, queue, connection_string, table):
        self.queue = queue
        self.connection_string = connection_string
        self.table = table

        engine = create_engine(self.connection_string, pool_recycle=1800)
        with Session(engine) as session:
            session.execute(
                f'''CREATE TABLE IF NOT EXISTS {self.table}
                    (
                        host VarChar(255) NOT NULL,
                        timestamp_start Integer, 
                        timestamp_end Integer, 
                        user Text, 
                        sent Integer, 
                        received Integer
                    )
                    PARTITION BY KEY (host)'''
            )
            session.commit()

    def run(self):
        engine = create_engine(self.connection_string, pool_recycle=1800)
        while True:
            try:
                sessionbytes = self.queue.get()
                for user in sessionbytes.data:
                    query = (
                        f"""INSERT INTO {self.table} 
                                (
                                    host,
                                    timestamp_start, 
                                    timestamp_end, 
                                    user, 
                                    sent, 
                                    received
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
            except ValueError as e:
                syslog.syslog(syslog.LOG_ERR, f"Error in DataWriter, {e}")
