import logging
import time
from typing import List, Dict
from concurrent.futures import ThreadPoolExecutor, as_completed

from domain.entities import TradingPair, Candle
from infrastructure.binance_client import BinanceClientInterface
from utils.batch_processor import BatchProcessor

logger = logging.getLogger(__name__)

class FetchHistoricalDataUseCase:
    def __init__(self, binance_client: BinanceClientInterface):
        self.binance_client = binance_client
        self.batch_processor = BatchProcessor()
    
    def execute(self, trading_pairs: List[TradingPair], interval: str, limit: int, 
                batch_size: int = 10, batch_delay: int = 1) -> None:
        
        logger.info(f"🔄 Obteniendo {limit} velas {interval} para {len(trading_pairs)} pares...")
        
        symbols = [pair.symbol for pair in trading_pairs]
        
        def fetch_single_symbol(symbol: str) -> List[Candle]:
            try:
                klines_data = self.binance_client.get_klines(symbol, interval, limit)
                if klines_data:
                    candles = self._process_klines(klines_data)
                    logger.info(f"✅ Velas obtenidas para {symbol}: {len(candles)} candles")
                    return candles
                else:
                    logger.warning(f"⚠️ No se pudieron obtener velas para {symbol}")
                    return []
            except Exception as e:
                logger.error(f"❌ Error procesando {symbol}: {e}")
                return []
        
        # Procesar en lotes usando el BatchProcessor
        results = self.batch_processor.process_batches(
            items=symbols,
            processor=fetch_single_symbol,
            batch_size=batch_size,
            delay=batch_delay
        )
        
        # Actualizar pares con velas
        candles_loaded = 0
        for symbol, candles in zip(symbols, results):
            pair = next((p for p in trading_pairs if p.symbol == symbol), None)
            if pair:
                pair.candles = candles
                if candles:
                    candles_loaded += 1
        
        logger.info(f"🎯 Velas cargadas para {candles_loaded}/{len(trading_pairs)} pares")
    
    def _process_klines(self, klines_data: List[List]) -> List[Candle]:
        candles = []
        for candle_data in klines_data:
            candle = Candle(
                time=int(candle_data[0]),
                human_time=time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(int(candle_data[0]) / 1000)),
                open=float(candle_data[1]),
                high=float(candle_data[2]),
                low=float(candle_data[3]),
                close=float(candle_data[4]),
                volume=float(candle_data[5]),
                close_time=int(candle_data[6]),
                quote_volume=float(candle_data[7]),
                trades=int(candle_data[8]),
                taker_buy_volume=float(candle_data[9]),
                taker_buy_quote_volume=float(candle_data[10])
            )
            candles.append(candle)
        return candles