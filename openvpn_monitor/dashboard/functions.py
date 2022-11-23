import datetime

import pandas as pd

from openvpn_monitor.columns import (
    HOST,
    USER,
    IP,
    CONNECTED_AT,
    CLOSED_AT,
    RECEIVED,
    SENT,
)
from openvpn_monitor.const import DATA_SPEEDS, DATA_SIZES


def get_sess_data(data: pd.DataFrame) -> pd.DataFrame:
    data = data.copy()
    data[CONNECTED_AT] = data[CONNECTED_AT].map(datetime.datetime.fromtimestamp)
    data[CONNECTED_AT] = data[CONNECTED_AT].map(lambda x: x.strftime("%Y-%m-%d %H:%M"))
    data[CLOSED_AT] = data[CLOSED_AT].map(datetime.datetime.fromtimestamp)
    data[CLOSED_AT] = data[CLOSED_AT].map(lambda x: x.strftime("%Y-%m-%d %H:%M"))
    data[RECEIVED] = data[RECEIVED].map(bytes_to_str)
    data[SENT] = data[SENT].map(bytes_to_str)
    return data[[HOST, USER, IP, CONNECTED_AT, CLOSED_AT, RECEIVED, SENT, ]]


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
