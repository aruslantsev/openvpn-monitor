from dataclasses import dataclass
from typing import Optional, Dict


@dataclass
class SessionData:
    host: str
    user: str
    ip: str
    internal_ip: str
    sent: int
    received: int
    connected_at_str: str
    connected_at: int
    closed_at: Optional[int] = None


@dataclass
class SessionBytes:
    host: str
    timestamp_start: int
    timestamp_end: int
    data: Dict[str, Dict[str, int]]
