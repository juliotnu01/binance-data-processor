import logging
from typing import Dict, List, Any, Optional
from config.settings import Config
from application.use_cases.calculate_rsi import CalculateRSIUseCase
from domain.entities import RSISignal

logger = logging.getLogger(__name__)

class RSIStateManager:
    def __init__(self, config: Config):
        self.config = config
        self.rsi_calculator = CalculateRSIUseCase(config)
        # Estado para cada símbolo: {symbol: {'last_close': float, 'avg_gain': float, 'avg_loss': float, 'previous_rsi': float}}
        self.rsi_state: Dict[str, Dict[str, float]] = {}
        # Buffer temporal para velas históricas por símbolo
        self.historical_buffer: Dict[str, List[Dict[str, Any]]] = {}
        self.initialized_symbols = set()

    def add_historical_candle(self, symbol: str, candle: Dict[str, Any]):
        """Añade una vela al buffer histórico de un símbolo."""
        if symbol not in self.historical_buffer:
            self.historical_buffer[symbol] = []
        self.historical_buffer[symbol].append(candle)

    def finalize_historical_initialization(self, symbol: str):
        """Calcula el estado RSI inicial para un símbolo usando su buffer histórico."""
        candles = self.historical_buffer.get(symbol)
        if not candles or len(candles) < self.config.rsi.period:
            logger.warning(f"⚠️ No hay suficientes velas históricicas para inicializar {symbol}. Se usará un estado neutro.")
            self.rsi_state[symbol] = { 'last_close': 0.0, 'avg_gain': 0.0, 'avg_loss': 0.0, 'previous_rsi': 50.0 }
            self.initialized_symbols.add(symbol)
            return

        logger.info(f"🧠 Calculando estado RSI inicial para {symbol} con {len(candles)} velas.")
        
        period = self.config.rsi.period
        # Calcular ganancias y pérdidas iniciales
        gains = 0.0
        losses = 0.0
        for i in range(1, len(candles)):
            change = candles[i]['close'] - candles[i-1]['close']
            gains += max(change, 0)
            losses += max(-change, 0)
        
        # Calcular promedios iniciales
        avg_gain = gains / period
        avg_loss = losses / period
        
        # Guardar estado inicial
        self.rsi_state[symbol] = {
            'last_close': candles[-1]['close'],
            'avg_gain': avg_gain,
            'avg_loss': avg_loss,
            'previous_rsi': 50.0
        }
        
        self.initialized_symbols.add(symbol)
        # Limpiar el buffer para liberar memoria
        if symbol in self.historical_buffer:
            del self.historical_buffer[symbol]
        
        logger.info(f"✅ Estado RSI para {symbol} inicializado.")

    def process_realtime_kline(self, event: Dict[str, Any]) -> Optional[RSISignal]:
        """Procesa una vela en tiempo real y calcula el nuevo RSI."""
        symbol = event.get('symbol')
        if symbol not in self.initialized_symbols:
            logger.warning(f"⚠️ Recibida vela en tiempo real para {symbol} antes de inicializar. Ignorando.")
            return None

        # Usamos el caso de uso para calcular la señal
        return self.rsi_calculator.process_kline_event(event)