import logging
import signal
import sys

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
        
        # El consumidor se suscribe a AMBOS tópicos
        self.consumer = KafkaRSIConsumer(
            config, 
            self.handle_message,
            topics=[config.kafka.topic_historical_klines, config.kafka.topic_realtime_klines]
        )
        self.running = True
        self.historical_data_loaded = False

        # Manejo de señales para shutdown graceful
        signal.signal(signal.SIGINT, self._signal_handler)
        signal.signal(signal.SIGTERM, self._signal_handler)

    def _signal_handler(self, signum, frame):
        logger.info(f"📡 Señal {signum} recibida, apagando el servicio RSI...")
        self.running = False
        self.consumer.stop()

    def handle_message(self, message) -> None:
        """Maneja cada mensaje consumido de Kafka, diferenciando entre histórico y real-time."""
        try:
            event = message.value
            event_type = event.get('event_type')
            symbol = event.get('symbol')

            if event_type == 'historical_kline':
                # Simplemente añadir la vela al buffer. No procesar todavía.
                self.state_manager.add_historical_candle(symbol, event)
                return

            if event_type == 'historical_data_loaded':
                # Este es el evento clave. Indica que ya no llegarán más velas históricas.
                # Ahora podemos inicializar el estado para todos los símbolos que hemos bufferizado.
                logger.info("📚 Evento 'historical_data_loaded' recibido. Inicializando estados RSI...")
                all_symbols = list(self.state_manager.historical_buffer.keys())
                for s in all_symbols:
                    self.state_manager.finalize_historical_initialization(s)
                
                self.historical_data_loaded = True
                logger.info(f"✅ Estados RSI inicializados para {len(all_symbols)} símbolos. Listo para datos en tiempo real.")
                return

            if event_type == 'realtime_kline':
                # Si los datos históricos no se han cargado, ignoramos mensajes en tiempo real.
                if not self.historical_data_loaded:
                    logger.warning(f"🚫 Ignorando vela en tiempo real para {symbol} porque los datos históricicos aún no se han cargado.")
                    return
                
                # Procesar la vela en tiempo real
                logger.debug(f"🕯️ Procesando vela en tiempo real para {symbol}")
                rsi_signal = self.state_manager.process_realtime_kline(event)
                
                if rsi_signal:
                    self.producer.publish(
                        self.config.kafka.output_topic, 
                        rsi_signal.__dict__, 
                        key=rsi_signal.symbol
                    )
                    logger.info(f"✅ Señal RSI para {rsi_signal.symbol} publicada. Valor: {rsi_signal.value:.2f}")
                return

            logger.warning(f"🚫 Mensaje con tipo de evento no manejado: {event_type}")

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
    setup_logging(logging.INFO)
    config = Config()
    service = RSIService(config)
    service.run()

if __name__ == "__main__":
    main()