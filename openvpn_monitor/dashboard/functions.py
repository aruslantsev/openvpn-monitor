from typing import Optional

from openvpn_monitor.constraints.const import DATA_SPEEDS, DATA_SIZES


def bytes_to_str(x: float) -> Optional[str]:
    """Converts bytes to KiB, MiB, GiB, ..."""
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


def speed_to_str(x: float) -> Optional[str]:
    """Converts bytes/s to KiB/s, MiB/s, GiB/s, ..."""
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
