import logging
import time
import signal
import sys
import asyncio

from config.settings import Config
from infrastructure.kafka_consumer import KafkaRSIConsumer
from infrastructure.kafka_producer import KafkaMessagePublisher
from application.use_cases.calculate_rsi import CalculateRSIUseCase
from utils.logger import setup_logging

logger = logging.getLogger(__name__)

class RSIService:
    def __init__(self, config: Config):
        self.config = config
        self.producer = KafkaMessagePublisher(config)
        self.rsi_calculator = CalculateRSIUseCase(config)
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
            # Solo procesamos eventos de kline cerrados para que el RSI sea consistente
            if event.get('event_type') == 'kline':
                logger.debug(f"🕯️ Procesando vela cerrada para {event.get('symbol')}")
                rsi_signal = self.rsi_calculator.process_kline_event(event)
                
                # Publicar la señal de RSI en el tópico de salida
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