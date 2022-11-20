import datetime

import pandas as pd

from openvpn_monitor.const import DATA_SPEEDS, DATA_SIZES


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
