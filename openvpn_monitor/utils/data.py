from dataclasses import dataclass
from typing import Optional


@dataclass
class SessionStats:
    user: str
    ip: str
    internal_ip: str
    sent: int
    received: int
    connected_at_str: str
    connected_at: int
    closed_at: Optional[int] = None
