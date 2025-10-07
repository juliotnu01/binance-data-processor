import asyncio
import logging
import signal
import sys
import threading

from config.settings import Config
from application.services.market_data_service import MarketDataService
from utils.logger import setup_logging

logger = logging.getLogger(__name__)

class DataIngestorApp:
    def __init__(self):
        self.config = Config()
        self.market_data_service = None
        self.running = True
        
        # Configurar manejo de señales
        signal.signal(signal.SIGINT, self._signal_handler)
        signal.signal(signal.SIGTERM, self._signal_handler)
    
    def _signal_handler(self, signum, frame):
        """Manejador de señales para shutdown graceful"""
        logger.info(f"📡 Señal {signum} recibida, apagando...")
        self.running = False
    
    async def run(self):
        """Ejecutar la aplicación"""
        try:
            logger.info("🚀 Iniciando Binance Data Ingestor")
            logger.info(f"📍 Kafka: {self.config.kafka.bootstrap_servers}")
            logger.info(f"📍 NATS: {self.config.nats.url}")
            logger.info(f"📊 Tópico: {self.config.kafka.topic_name}")
            logger.info(f"⏱️  Agregación: {self.config.app.aggregation_interval}s")
            
            # Inicializar servicio
            self.market_data_service = MarketDataService(self.config)
            await self.market_data_service.initialize()
            
            logger.info("🎯 Data Ingestor funcionando correctamente")
            
            # Mantener la aplicación corriendo
            while self.running:
                await asyncio.sleep(1)
            
        except Exception as e:
            logger.error(f"🚨 Error crítico: {e}")
            raise
        finally:
            await self.shutdown()
    
    async def shutdown(self):
        """Apagar la aplicación"""
        if self.market_data_service:
            await self.market_data_service.shutdown()
        logger.info("👋 Data Ingestor apagado correctamente")

async def main():
    """Función principal"""
    setup_logging()
    app = DataIngestorApp()
    await app.run()

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("👋 Apagado por usuario")
    except Exception as e:
        logger.error(f"💥 Error fatal: {e}")
        sys.exit(1)