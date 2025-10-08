import logging
import json
from typing import Dict, Any

from domain.entities import MarketData
from infrastructure.kafka_producer import KafkaMessagePublisher # <-- Añadir importación

logger = logging.getLogger(__name__)

class StreamMarketDataUseCase:
    def __init__(self, market_data: MarketData, kafka_publisher: KafkaMessagePublisher): # <-- Añadir publisher
        self.market_data = market_data
        self.kafka_publisher = kafka_publisher # <-- Guardar publisher
    
    def handle_kline_message(self, data: Dict[str, Any]) -> None:
        """Maneja mensajes de klines del WebSocket y los publica en Kafka con clave."""
        logger.debug(f"🕯️ Entrando en el manejador de klines con datos: {data}")
        try:
            if 'data' not in data or 'k' not in data['data']:
                logger.warning("🚨 Mensaje de kline recibido pero no tiene la estructura esperada ('data' o 'k').")
                return
            
            kline_data = data['data']['k']
            symbol = kline_data['s']
            is_kline_closed = kline_data['x']
            event_type = data['data']['e']

            logger.info(f"🕯️ Vela recibida para {symbol}. ¿Cerrada?: {is_kline_closed}. Precio: {kline_data['c']}")

            candle_event = {
                'event_type': event_type,
                'symbol': symbol,
                'is_closed': is_kline_closed,
                'time': int(kline_data['t']),
                'open': float(kline_data['o']),
                'high': float(kline_data['h']),
                'low': float(kline_data['l']),
                'close': float(kline_data['c']),
                'volume': float(kline_data['v']),
                'quote_volume': float(kline_data['q']),
                'trades': int(kline_data['n']),
                'taker_buy_volume': float(kline_data['V']),
                'taker_buy_quote_volume': float(kline_data['Q']),
                'timestamp': int(kline_data['T'])
            }

            topic = self.kafka_publisher.config.kafka.topic_name
            if self.kafka_publisher.publish(topic, candle_event, key=symbol):
                logger.info(f"✅ Vela de {symbol} enviada a Kafka. Clave: {symbol}")
            else:
                logger.error(f"❌ Fallo al publicar vela para {symbol} en Kafka")
                
        except Exception as e:
            logger.error(f"❌ Error procesando mensaje de kline: {e}")