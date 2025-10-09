import logging
import time

from typing import Dict, Any
from domain.entities import RSISignal
from config.settings import Config

logger = logging.getLogger(__name__)

class CalculateRSIUseCase:
    def __init__(self, config: Config):  
        self.config = config  

    def process_kline_event(self, event: Dict[str, Any], state: Dict[str, float]) -> RSISignal:  
        """Procesa un evento de kline y calcula el nuevo RSI usando el estado proporcionado."""  
        symbol = event.get('symbol')  
        close_price = event.get('close')  
        kline_timestamp = event.get('timestamp')  
        if not symbol or close_price is None:  
            raise ValueError("El evento de kline no tiene 'symbol' o 'close'.")  
        last_close = state['last_close']  
        previous_rsi = state['previous_rsi']  
        # Calcular ganancia/pérdida  
        change = close_price - last_close  
        gain = max(change, 0)  
        loss = max(-change, 0)  
        # Suavizado (Wilder's Smoothing)  
        period = self.config.rsi.period  
        avg_gain = (state['avg_gain'] * (period - 1) + gain) / period  
        avg_loss = (state['avg_loss'] * (period - 1) + loss) / period  
        # Actualizar estado  
        state['last_close'] = close_price  
        state['avg_gain'] = avg_gain  
        state['avg_loss'] = avg_loss  
        # Calcular RSI  
        if avg_loss == 0:  
            rsi_value = 100.0  
        elif avg_gain == 0:  
            rsi_value = 0.0  
        else:  
            rs = avg_gain / avg_loss  
            rsi_value = 100 - (100 / (1 + rs))  
        state['previous_rsi'] = rsi_value  
        return self._create_rsi_signal(symbol, rsi_value, previous_rsi, kline_timestamp)

    def _create_rsi_signal(self, symbol: str, rsi_value: float, previous_rsi: float, kline_timestamp: int) -> RSISignal:
        """Crea el objeto RSISignal con el análisis completo."""
        momentum = "ascendente" if rsi_value > previous_rsi else "descendente"
        
        # Lógica de análisis (traducida de tu Pinia store)
        if rsi_value > 70:
            strength = "extrema" if rsi_value > 80 else "fuerte"
            if momentum == "ascendente":
                status = f"Sobrecompra {strength} (Momentum positivo - Precaución)"
                color = "purple" if rsi_value > 80 else "red"
            else:
                status = f"Sobrecompra {strength} (Momentum negativo - Posible reversión)"
                color = "dark-red"
        elif rsi_value < 30:
            strength = "extrema" if rsi_value < 20 else "fuerte"
            if momentum == "descendente":
                status = f"Sobreventa {strength} (Momentum negativo - Precaución)"
                color = "blue" if rsi_value < 20 else "green"
            else:
                status = f"Sobreventa {strength} (Momentum positivo - Posible rebote)"
                color = "dark-green"
        else:
            trend = "alcista" if rsi_value > 50 else "bajista"
            status = f"Mercado neutral con tendencia {trend}"
            color = "light-green" if rsi_value > 50 else "light-red"
            strength = "moderada"

        return RSISignal(
            symbol=symbol,
            value=rsi_value,
            previous_rsi=previous_rsi,
            status=status,
            color=color,
            momentum=momentum,
            strength=strength,
            timestamp=int(time.time() * 1000),
            kline_timestamp=kline_timestamp
        )