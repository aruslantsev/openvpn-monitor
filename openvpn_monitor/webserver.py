import datetime
import os

from dash import Dash, html, dcc, dash_table
from dash.dependencies import Output, Input

from functions import OVPNDataReader, OVPNSessionsReader, bytes_to_str, speed_to_str, get_sess_data

connection_string = os.environ['CONNECTION_STRING']

datareader = OVPNDataReader(conn_string=connection_string, table="data")
sessionreader = OVPNSessionsReader(conn_string=connection_string, table="sessions")

timedeltas = {
    "15m": datetime.timedelta(minutes=15),
    "1h": datetime.timedelta(hours=1),
    "12h": datetime.timedelta(hours=12),
    "1d": datetime.timedelta(days=1),
}

app = Dash(__name__)
app.title = "OpenVPN Monitor"

app.layout = html.Div(children=[
    html.H3(children='OpenVPN monitoring'),

    html.H4(children="Traffic since month start"),
    dash_table.DataTable(
        id="traffic-month",
        cell_selectable=False,
        fill_width=False,
    ),

    html.H4(children="Active users"),
    dash_table.DataTable(
        id="active-users",
        cell_selectable=False,
        fill_width=False,
    ),

    html.H4(children="Traffic"),
    dash_table.DataTable(
        id="traffic",
        cell_selectable=False,
        # fill_width=False,
    ),

    html.H4(children="Average speed"),
    dash_table.DataTable(
        id="speed",
        cell_selectable=False,
        # fill_width=False,
    ),

    html.H4(children="Latest closed sessions"),
    dash_table.DataTable(id='sessions'),

    dcc.Interval(
        id='interval-component',
        interval=600 * 1000  # ms
    ),
])


@app.callback(
    Output('traffic-month', 'data'),
    Input('interval-component', 'n_intervals')
)
def traffic_1month_updater(_):
    curr_date = datetime.datetime.now()
    start_date = datetime.datetime(
        year=curr_date.year, month=curr_date.month, day=1, hour=0, minute=0, second=0)
    data = datareader(connected_at_min=start_date.timestamp())

    users = (
        data[['user']]
        .drop_duplicates()
        .reset_index(drop=True)
        .sort_values('user')
    )

    received = data.groupby('user')['received'].sum().reset_index()
    sent = data.groupby('user')['sent'].sum().reset_index()

    users = (
        users
        .merge(received, how="left", on="user")
        .merge(sent, how="left", on="user")
    )

    users['received'] = users['received'].map(bytes_to_str)
    users['sent'] = users['sent'].map(bytes_to_str)

    return users.to_dict("records")


@app.callback(
    Output('traffic', 'data'),
    Input('interval-component', 'n_intervals')
)
def traffic_updater(_):
    curr_date = datetime.datetime.now()
    start_date = curr_date - datetime.timedelta(days=1)
    data = datareader(connected_at_min=start_date.timestamp())

    users = (
        data[['user']]
        .drop_duplicates()
        .reset_index(drop=True)
        .sort_values('user')
    )

    for time_prefix, timedelta in timedeltas.items():
        data_tmp = data[data['timestamp_start'] >= (curr_date - timedelta).timestamp()]

        received = data_tmp.groupby('user')['received'].sum().reset_index()
        sent = data_tmp.groupby('user')['sent'].sum().reset_index()

        received['received'] = received['received'].map(bytes_to_str)
        sent['sent'] = sent['sent'].map(bytes_to_str)

        received = received.rename(columns={"received": f"received_{time_prefix}"})
        sent = sent.rename(columns={"sent": f"sent_{time_prefix}"})

        users = (
            users
            .merge(received, how="left", on="user")
            .merge(sent, how="left", on="user")
            .rename(columns={"received": f"received_{time_prefix}", "sent": f"sent_{time_prefix}"})
        )

    return users.to_dict("records")


@app.callback(
    Output('speed', 'data'),
    Input('interval-component', 'n_intervals')
)
def speed_updater(_):
    curr_date = datetime.datetime.now()
    start_date = curr_date - datetime.timedelta(days=1)
    data = datareader(connected_at_min=start_date.timestamp())

    users = (
        data[['user']]
        .drop_duplicates()
        .reset_index(drop=True)
        .sort_values('user')
    )

    for time_prefix, timedelta in timedeltas.items():
        data_tmp = data[data['timestamp_start'] >= (curr_date - timedelta).timestamp()]

        received = data_tmp.groupby('user')['received'].sum().reset_index()
        sent = data_tmp.groupby('user')['sent'].sum().reset_index()

        seconds = timedelta.seconds
        if time_prefix == "1d":
            seconds = 86400

        received['received'] = received['received'] / seconds
        sent['sent'] = sent['sent'] / seconds

        received['received'] = received['received'].map(speed_to_str)
        sent['sent'] = sent['sent'].map(speed_to_str)

        received = received.rename(columns={"received": f"received_{time_prefix}"})
        sent = sent.rename(columns={"sent": f"sent_{time_prefix}"})

        users = (
            users
            .merge(received, how="left", on="user")
            .merge(sent, how="left", on="user")
            .rename(columns={"received": f"received_{time_prefix}", "sent": f"sent_{time_prefix}"})
        )

    return users.to_dict("records")


@app.callback(
    Output('active-users', 'data'),
    Input('interval-component', 'n_intervals')
)
def users_updater(_):
    data = datareader(
        connected_at_min=(datetime.datetime.now() - datetime.timedelta(minutes=1)).timestamp())
    users = data[['user']].drop_duplicates()
    users = users[users['user'] != "__ALL__"].reset_index(drop=True)

    return users.to_dict("records")


@app.callback(
    Output('sessions', 'data'),
    Input('interval-component', 'n_intervals')
)
def update_sent_graph(_):
    sessions = sessionreader(limit=10)
    sessions = get_sess_data(sessions)

    return sessions.to_dict("records")


if __name__ == '__main__':
    app.run_server(port=8888, host="0.0.0.0", )
