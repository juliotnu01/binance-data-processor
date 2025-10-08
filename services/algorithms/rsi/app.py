import logging
import sys
import signal
import asyncio

from config.settings import Config
from application.services.rsi_service import RSIService
from utils.logger import setup_logging

# Configuración del logging
setup_logging(logging.INFO)
logger = logging.getLogger(__name__)

class AlgorithmRSIApp:
    def __init__(self):
        self.config = Config()
        self.service = RSIService(self.config)
        self.running = True

        # Configurar manejo de señales para shutdown graceful
        signal.signal(signal.SIGINT, self._signal_handler)
        signal.signal(signal.SIGTERM, self._signal_handler)

    def _signal_handler(self, signum, frame):
        """Manejador de señales para shutdown graceful."""
        logger.info(f"📡 Señal {signum} recibida, apagando la aplicación RSI...")
        self.running = False
        # El servicio ya tiene su propia lógica de parada, pero la señal se propaga aquí.
        self.service.consumer.stop()

    def run(self):
        """Ejecutar la aplicación."""
        try:
            logger.info("🚀 Iniciando Algorithm RSI App")
            logger.info(f"📍 Kafka Bootstrap Servers: {self.config.kafka.bootstrap_servers}")
            logger.info(f"📥 Input Topic: {self.config.kafka.input_topic}")
            logger.info(f"📤 Output Topic: {self.config.kafka.output_topic}")
            logger.info(f"🧮 RSI Period: {self.config.rsi.period}")
            
            # El servicio se ejecuta en un bucle bloqueante, así que no necesitamos asyncio aquí.
            self.service.run()

        except Exception as e:
            logger.error(f"🚨 Error crítico en la aplicación RSI: {e}")
            raise
        finally:
            logger.info("👋 Algorithm RSI App apagada correctamente.")

if __name__ == "__main__":
    app = AlgorithmRSIApp()
    app.run()