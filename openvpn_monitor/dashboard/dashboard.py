import datetime
import os

import plotly.express as px
from dash import Dash, html, dcc, dash_table
from dash.dependencies import Output, Input

from openvpn_monitor.columns import (
    HOST,
    USER,
    SENT,
    RECEIVED,
    TIMESTAMP_START,
    TIMESTAMP_END,
)
from openvpn_monitor.const import TIMEDELTAS, ALL
from openvpn_monitor.dashboard.functions import (
    bytes_to_str,
    speed_to_str,
    get_sess_data,
)
from openvpn_monitor.dashboard.sql import (
    OVPNDataReader,
    OVPNSessionsReader,
    OVPNHostsReader,
)
from openvpn_monitor.tables import DATA_TABLE, SESSIONS_TABLE

connection_string = os.environ['CONNECTION_STRING']

datareader = OVPNDataReader(conn_string=connection_string, table=DATA_TABLE)
sessionreader = OVPNSessionsReader(conn_string=connection_string, table=SESSIONS_TABLE)
hostsreader = OVPNHostsReader(conn_string=connection_string, table=SESSIONS_TABLE)

app = Dash(__name__, title="OpenVPN Monitor")

TIMER = "timer"
TIME_UPDATED = "time_updated"
TIME_PERIOD_SELECTOR = "time_period_selector"
HOST_SELECTOR = "host_selector"
TRAFFIC_SINCE_MONTH_START_TABLE = "traffic_since_month_start_table"
ACTIVE_USERS_TABLE = "active_users_table"
TRAFFIC_FOR_TIME_PERIOD_TABLE = "traffic_for_time_period_table"
SPEED_FOR_TIME_PERIOD_TABLE = "speed_for_time_period_table"
CLOSED_SESSIONS_TABLE = "closed_sessions_table"
RECEIVED_GRAPH = "received_graph"
SENT_GRAPH = "sent_graph"

TIME_DIFF = "time_diff"

app.layout = html.Div(
    children=[
        dcc.Interval(
            id=TIMER,
            interval=60 * 1000  # ms
        ),

        html.H3(children='OpenVPN monitoring'),
        html.Br(),

        html.Div(id=TIME_UPDATED),

        html.Div(
            children=[
                html.Div(
                    children=[
                        html.H4(children="Time period"),
                        dcc.Dropdown(
                            id=TIME_PERIOD_SELECTOR,
                            options=list(TIMEDELTAS.keys()),
                            placeholder="Select time period",
                            value="inf",
                            clearable=False,
                        ),
                    ],
                    style={'padding': 10, 'flex': 1}
                ),
                html.Div(
                    children=[
                        html.H4(children="Hosts"),
                        dcc.Dropdown(
                            id=HOST_SELECTOR,
                            placeholder="Select OpenVPN server",
                            value=ALL,
                            clearable=False,
                        ),
                    ],
                    style={'padding': 10, 'flex': 1}
                ),
            ],
            style={'display': 'flex', 'flex-direction': 'row'}
        ),
        html.Br(),

        html.Div(
            children=[
                html.Div(
                    children=[
                        html.H4(children="Traffic since month start"),
                        dash_table.DataTable(
                            id=TRAFFIC_SINCE_MONTH_START_TABLE,
                            cell_selectable=False,
                            fill_width=False,
                        ),
                    ],
                    style={'padding': 10, 'flex': 1}
                ),
                html.Div(
                    children=[
                        html.H4(children="Active users"),
                        dash_table.DataTable(
                            id=ACTIVE_USERS_TABLE,
                            cell_selectable=False,
                            fill_width=False,
                        ),
                    ],
                    style={'padding': 10, 'flex': 1}
                )
            ],
            style={'display': 'flex', 'flex-direction': 'row'}
        ),
        html.Br(),

        html.H4(children="Traffic"),
        dash_table.DataTable(
            id=TRAFFIC_FOR_TIME_PERIOD_TABLE,
            cell_selectable=False,
            # fill_width=False,
        ),

        html.H4(children="Average speed"),
        dash_table.DataTable(
            id=SPEED_FOR_TIME_PERIOD_TABLE,
            cell_selectable=False,
            # fill_width=False,
        ),
        html.Br(),

        html.H4(children="Latest closed sessions"),
        dash_table.DataTable(id=CLOSED_SESSIONS_TABLE),
        html.Br(),

        html.H4(children="Received speed"),
        dcc.Graph(id=RECEIVED_GRAPH),

        html.H4(children="Sent speed"),
        dcc.Graph(id=SENT_GRAPH),
    ]
)


@app.callback(
    Output(HOST_SELECTOR, "options"),
    Input(TIME_PERIOD_SELECTOR, "value"),
    Input(TIMER, "n_intervals"),
)
def all_hosts_update(timedelta_str, _):
    timedelta = TIMEDELTAS[timedelta_str]
    return [ALL] + hostsreader(timedelta=timedelta)


@app.callback(
    Output(TIME_UPDATED, "children"),
    Input(TIMER, "n_intervals"),
)
def time_update(_):
    return f"""Last update: {datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")}"""


@app.callback(
    Output(ACTIVE_USERS_TABLE, "data"),
    Input(HOST_SELECTOR, "value"),
    Input(TIMER, "n_intervals"),
)
def active_users_table(host, _):
    if host == ALL:
        host = None
    data = datareader(
        host=host,
        connected_at_min=datetime.datetime.now() - datetime.timedelta(minutes=5)
    )
    users = data[[USER, HOST]].drop_duplicates()
    users = users[users[USER] != ALL].reset_index(drop=True)

    return users.to_dict("records")


@app.callback(
    Output(TRAFFIC_SINCE_MONTH_START_TABLE, "data"),
    Input(HOST_SELECTOR, "value"),
    Input(TIMER, "n_intervals"),
)
def traffic_since_month_start_table(host, _):
    if host == ALL:
        host = None
    curr_date = datetime.datetime.now()
    start_date = datetime.datetime(
        year=curr_date.year, month=curr_date.month, day=1, hour=0, minute=0, second=0)
    data = datareader(host=host, connected_at_min=start_date)

    users = (
        data[[HOST, USER]]
        .drop_duplicates()
        .reset_index(drop=True)
        .sort_values('user')
    )

    received = data.groupby([HOST, USER])[RECEIVED].sum().reset_index()
    sent = data.groupby([HOST, USER])[SENT].sum().reset_index()

    users = (
        users
        .merge(received, how="left", on=[HOST, USER])
        .merge(sent, how="left", on=[HOST, USER])
    )

    users[RECEIVED] = users[RECEIVED].map(bytes_to_str)
    users[SENT] = users[SENT].map(bytes_to_str)

    return users.to_dict("records")


@app.callback(
    Output(TRAFFIC_FOR_TIME_PERIOD_TABLE, "data"),
    Input(TIME_PERIOD_SELECTOR, "value"),
    Input(HOST_SELECTOR, "value"),
    Input(TIMER, "n_intervals"),
)
def traffic_for_time_period_table(timedelta_str, host, _):
    if host == ALL:
        host = None

    curr_date = datetime.datetime.now()
    timedelta = TIMEDELTAS[timedelta_str]
    if timedelta is not None:
        start_date = curr_date - TIMEDELTAS[timedelta_str]
    else:
        start_date = None
    data = datareader(connected_at_min=start_date, host=host)

    users = (
        data[[HOST, USER]]
        .drop_duplicates()
        .reset_index(drop=True)
        .sort_values(USER)
    )

    received = data.groupby([HOST, USER])[RECEIVED].sum().reset_index()
    sent = data.groupby([HOST, USER])[SENT].sum().reset_index()

    received[RECEIVED] = received[RECEIVED].map(bytes_to_str)
    sent[SENT] = sent[SENT].map(bytes_to_str)

    users = (
        users
        .merge(received, how="left", on=[HOST, USER])
        .merge(sent, how="left", on=[HOST, USER])
    )

    return users.to_dict("records")


@app.callback(
    Output(SPEED_FOR_TIME_PERIOD_TABLE, "data"),
    Input(TIME_PERIOD_SELECTOR, "value"),
    Input(HOST_SELECTOR, "value"),
    Input(TIMER, "n_intervals"),
)
def speed_for_time_period_table(timedelta_str, host, _):
    if host == ALL:
        host = None

    curr_date = datetime.datetime.now()
    timedelta = TIMEDELTAS[timedelta_str]
    if timedelta is not None:
        start_date = curr_date - TIMEDELTAS[timedelta_str]
    else:
        start_date = None

    data = datareader(connected_at_min=start_date, host=host)

    users = (
        data[[HOST, USER]]
        .drop_duplicates()
        .reset_index(drop=True)
        .sort_values([HOST, USER])
    )

    received = data.groupby([HOST, USER])[RECEIVED].sum().reset_index()
    sent = data.groupby([HOST, USER])[SENT].sum().reset_index()

    if timedelta is not None:
        seconds = timedelta.total_seconds()
    else:
        seconds = datetime.datetime.now().timestamp() - data[TIMESTAMP_START].min()

    received[RECEIVED] = received[RECEIVED] / seconds
    sent[SENT] = sent[SENT] / seconds

    received[RECEIVED] = received[RECEIVED].map(speed_to_str)
    sent[SENT] = sent[SENT].map(speed_to_str)

    users = (
        users
        .merge(received, how="left", on=[HOST, USER])
        .merge(sent, how="left", on=[HOST, USER])
    )

    return users.to_dict("records")


@app.callback(
    Output(CLOSED_SESSIONS_TABLE, "data"),
    Input(HOST_SELECTOR, "value"),
    Input(TIMER, "n_intervals"),
)
def closed_sessions_table(host, _):
    if host == ALL:
        host = None
    sessions = sessionreader(host=host, limit=20)
    sessions = get_sess_data(sessions)

    return sessions.to_dict("records")


@app.callback(
    Output(RECEIVED_GRAPH, "figure"),
    Input(TIME_PERIOD_SELECTOR, "value"),
    Input(HOST_SELECTOR, "value"),
    Input(TIMER, "n_intervals"),
)
def received_graph(timedelta_str, host, _):
    if host == ALL:
        host = None

    curr_date = datetime.datetime.now()
    timedelta = TIMEDELTAS[timedelta_str]
    if timedelta is not None:
        start_date = curr_date - TIMEDELTAS[timedelta_str]
    else:
        start_date = None

    data = datareader(connected_at_min=start_date, host=host)
    data[TIME_DIFF] = data[TIMESTAMP_END] - data[TIMESTAMP_START]
    data[RECEIVED] = data[RECEIVED] / data[TIME_DIFF]
    data[SENT] = data[SENT] / data[TIME_DIFF]
    data[USER + HOST] = data[USER] + data[HOST]

    graph = px.line(data, x=TIMESTAMP_START, y=RECEIVED, color=USER + HOST)
    return graph


@app.callback(
    Output(SENT_GRAPH, "figure"),
    Input(TIME_PERIOD_SELECTOR, "value"),
    Input(HOST_SELECTOR, "value"),
    Input(TIMER, "n_intervals"),
)
def sent_graph(timedelta_str, host, _):
    if host == ALL:
        host = None

    curr_date = datetime.datetime.now()
    timedelta = TIMEDELTAS[timedelta_str]
    if timedelta is not None:
        start_date = curr_date - TIMEDELTAS[timedelta_str]
    else:
        start_date = None

    data = datareader(connected_at_min=start_date, host=host)
    data[TIME_DIFF] = data[TIMESTAMP_END] - data[TIMESTAMP_START]
    data[RECEIVED] = data[RECEIVED] / data[TIME_DIFF]
    data[SENT] = data[SENT] / data[TIME_DIFF]
    data[USER + HOST] = data[USER] + data[HOST]

    graph = px.line(data, x=TIMESTAMP_START, y=SENT, color=USER + HOST)
    return graph
