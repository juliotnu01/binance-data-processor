import json
import logging
from typing import Any, Dict
import asyncio
import nats
# from nats.errors import TimeoutError # Ya no lo necesitamos para esta opción

from config.settings import Config

logger = logging.getLogger(__name__)

class NATSPublisher:
    def __init__(self, config: Config):
        self.config = config
        self.nc = None
        # self.js = None  <-- Ya no necesitamos JetStream
    
    async def connect(self) -> None:
        """Conectar a NATS"""
        try:
            self.nc = await nats.connect(self.config.nats.url)
            # self.js = self.nc.jetstream() <-- Comentamos o borramos esto
            logger.info("✅ Conectado a NATS exitosamente!")
        except Exception as e:
            logger.error(f"❌ Error conectando a NATS: {e}")
            raise
    
    async def publish_market_data(self, subject: str, data: Dict[str, Any]) -> bool:
        """Publicar datos de mercado via NATS (sin JetStream)"""
        try:
            if self.nc:  # Usamos self.nc en lugar de self.js
                subject_name = f"{self.config.nats.subject_prefix}.{subject}"
                # Cambiamos de js.publish a nc.publish
                await self.nc.publish(subject_name, json.dumps(data).encode())
                logger.debug(f"📨 Publicado en NATS: {subject_name}")
                return True
        except Exception as e:
            logger.error(f"❌ Error publicando en NATS: {e}")
        return False
    
    async def close(self) -> None:
        """Cerrar conexión NATS"""
        if self.nc:
            await self.nc.close()