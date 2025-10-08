import logging
from typing import Dict, List, Any

from application.use_cases.calculate_rsi import CalculateRSIUseCase
from domain.entities import RSISignal

logger = logging.getLogger(__name__)

class RSIStateManager:
    def __init__(self, config: Config):
        self.config = config
        self.rsi_calculator = CalculateRSIUseCase(config)
        # Estado para cada símbolo: {symbol: {'last_close': float, 'avg_gain': float, 'avg_loss': float, 'previous_rsi': float}}
        self.rsi_state: Dict[str, Dict[str, float]] = {}
        self.initialized_symbols = set()

    def process_historical_batch(self, symbol: str, candles: List[Dict[str, Any]]) -> None:
        """Procesa un lote de velas históricas para inicializar el estado de un símbolo."""
        logger.info(f"🧠 Inicializando estado RSI para {symbol} con {len(candles)} velas históricas.")
        if not candles:
            return
        
        # Reiniciar estado para el símbolo
        self.rsi_state[symbol] = {
            'last_close': candles[0]['close'],
            'avg_gain': 0.0,
            'avg_loss': 0.0,
            'previous_rsi': 50.0
        }

        # Procesar todas las velas históricas para calcular los promedios iniciales
        for candle in candles:
            change = candle['close'] - self.rsi_state[symbol]['last_close']
            gain = max(change, 0)
            loss = max(-change, 0)
            period = self.config.rsi.period
            self.rsi_state[symbol]['avg_gain'] = (self.rsi_state[symbol]['avg_gain'] * (period - 1) + gain) / period
            self.rsi_state[symbol]['avg_loss'] = (self.rsi_state[symbol]['avg_loss'] * (period - 1) + loss) / period
            self.rsi_state[symbol]['last_close'] = candle['close']
        
        self.initialized_symbols.add(symbol)
        logger.info(f"✅ Estado RSI para {symbol} inicializado.")

    def process_realtime_kline(self, event: Dict[str, Any]) -> RSISignal:
        """Procesa una vela en tiempo real y calcula el nuevo RSI."""
        symbol = event.get('symbol')
        if symbol not in self.initialized_symbols:
            logger.warning(f"⚠️ Recibida vela en tiempo real para {symbol} antes de inicializar su estado. Ignorando.")
            return None

        # Reutilizamos la lógica que ya tenías
        return self.rsi_calculator.process_kline_event(event)