from dataclasses import dataclass
from typing import Optional

@dataclass
class RSISignal:
    symbol: str
    value: float
    previous_rsi: float
    status: str
    color: str
    momentum: str
    strength: str
    timestamp: int
    kline_timestamp: int