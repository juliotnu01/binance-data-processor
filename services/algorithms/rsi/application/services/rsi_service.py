import logging
import time
import signal
import sys
import asyncio

from config.settings import Config
from infrastructure.kafka_consumer import KafkaRSIConsumer
from infrastructure.kafka_producer import KafkaMessagePublisher
from application.services.rsi_state_manager import RSIStateManager
from utils.logger import setup_logging

logger = logging.getLogger(__name__)

class RSIService:
    def __init__(self, config: Config):
        self.config = config
        self.producer = KafkaMessagePublisher(config)
        self.state_manager = RSIStateManager(config)
        self.consumer = KafkaRSIConsumer(config, self.handle_message)
        self.running = True

        # Manejo de señales para shutdown graceful
        signal.signal(signal.SIGINT, self._signal_handler)
        signal.signal(signal.SIGTERM, self._signal_handler)

    def _signal_handler(self, signum, frame):
        logger.info(f"📡 Señal {signum} recibida, apagando el servicio RSI...")
        self.running = False
        self.consumer.stop()

    def handle_message(self, message) -> None:
        """Maneja cada mensaje consumido de Kafka."""
        try:
            event = message.value
            event_type = event.get('event_type')
            symbol = event.get('symbol')

            if event_type == 'historical_kline':
                # Este es un mensaje del lote histórico, lo ignoramos por ahora
                # La inicialización se hará con ksqlDB
                logger.debug(f"📜 Ignorando vela histórica individual para {symbol}. La inicialización se hará con ksqlDB.")
                return

            if event_type == 'historical_data_loaded':
                # Señal de que el lote histórico está completo. Aquí es donde ksqlDB haría su trabajo.
                # Por ahora, lo omitimos.
                logger.info("📚 Señal de datos históricos cargados recibida.")
                return

            if event_type == 'realtime_klines':
                logger.debug(f"🕯️ Procesando vela en tiempo real para {symbol}")
                rsi_signal = self.state_manager.process_realtime_kline(event)
                
                if rsi_signal:
                    self.producer.publish(
                        self.config.kafka.output_topic, 
                        rsi_signal.__dict__, 
                        key=rsi_signal.symbol
                    )
                    logger.info(f"✅ Señal RSI para {rsi_signal.symbol} publicada. Valor: {rsi_signal.value:.2f}")
        except Exception as e:
            logger.error(f"❌ Error en handle_message: {e}")

    def run(self):
        """Inicia el servicio."""
        logger.info("🚀 Iniciando Algorithm RSI Service")
        try:
            self.consumer.run()
        except KeyboardInterrupt:
            logger.info("👋 Servicio RSI detenido por el usuario.")
        except Exception as e:
            logger.error(f"🚨 Error crítico en el servicio RSI: {e}")
        finally:
            logger.info("👋 Servicio RSI apagado correctamente.")

def main():
    """Función principal."""
    setup_logging(logging.INFO)
    config = Config()
    service = RSIService(config)
    service.run()

if __name__ == "__main__":
    main()