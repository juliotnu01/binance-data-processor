from dataclasses import dataclass, field
from typing import List, Dict, Any, Optional
from datetime import datetime

@dataclass
class Candle:
    time: int
    human_time: str
    open: float
    high: float
    low: float
    close: float
    volume: float
    close_time: int
    quote_volume: float
    trades: int
    taker_buy_volume: float
    taker_buy_quote_volume: float

@dataclass
class TradingPair:
    symbol: str
    base_asset: str
    quote_asset: str
    filters: List[Dict[str, Any]]
    candles: List[Candle] = field(default_factory=list)
    max_leverage: int = 0
    
    def update_candle(self, kline_data: Dict[str, Any]) -> None:
        """Actualiza o añade una vela al par de trading"""
        candle_time = int(kline_data['t'])
        
        # Buscar si ya existe una vela para este tiempo
        existing_candle = next((c for c in self.candles if c.time == candle_time), None)
        
        if existing_candle:
            # Actualizar vela existente
            existing_candle.close = float(kline_data['c'])
            existing_candle.high = max(existing_candle.high, float(kline_data['h']))
            existing_candle.low = min(existing_candle.low, float(kline_data['l']))
            existing_candle.volume = float(kline_data['v'])
            existing_candle.close_time = int(kline_data['T'])
            existing_candle.quote_volume = float(kline_data['q'])
            existing_candle.trades = int(kline_data['n'])
            existing_candle.taker_buy_volume = float(kline_data['V'])
            existing_candle.taker_buy_quote_volume = float(kline_data['Q'])
        else:
            # Añadir nueva vela
            new_candle = Candle(
                time=candle_time,
                human_time=datetime.fromtimestamp(candle_time / 1000).strftime('%Y-%m-%d %H:%M:%S'),
                open=float(kline_data['o']),
                high=float(kline_data['h']),
                low=float(kline_data['l']),
                close=float(kline_data['c']),
                volume=float(kline_data['v']),
                close_time=int(kline_data['T']),
                quote_volume=float(kline_data['q']),
                trades=int(kline_data['n']),
                taker_buy_volume=float(kline_data['V']),
                taker_buy_quote_volume=float(kline_data['Q'])
            )
            self.candles.append(new_candle)
            
            # Mantener solo las últimas N velas
            max_candles = 1000
            if len(self.candles) > max_candles:
                self.candles = self.candles[-max_candles:]

@dataclass
class MarketData:
    pairs: List[TradingPair] = field(default_factory=list)
    
    def get_pair(self, symbol: str) -> Optional[TradingPair]:
        """Obtiene un par de trading por su símbolo"""
        return next((p for p in self.pairs if p.symbol == symbol), None)
    
    def update_pair_candle(self, symbol: str, kline_data: Dict[str, Any]) -> bool:
        """Actualiza la vela de un par de trading"""
        pair = self.get_pair(symbol)
        if pair:
            pair.update_candle(kline_data)
            return True
        return False