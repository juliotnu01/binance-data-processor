import json
import logging
from typing import Any, Dict, Optional
from abc import ABC, abstractmethod
from kafka import KafkaProducer
from kafka.errors import KafkaError

from config.settings import Config

logger = logging.getLogger(__name__)

class MessagePublisher(ABC):
    @abstractmethod
    def publish(self, topic: str, message: Dict[str, Any], key: Optional[str] = None) -> bool:
        pass
    
    @abstractmethod
    def flush(self) -> None:
        pass

class KafkaMessagePublisher(MessagePublisher):
    def __init__(self, config: Config):
        self.config = config
        self.producer = None
        self._initialize_producer()
    
    def _initialize_producer(self) -> None:
        max_retries = 10
        for i in range(max_retries):
            try:
                logger.info(f"🔄 Intento {i+1}/{max_retries} de conectar a Kafka...")
                self.producer = KafkaProducer(
                    bootstrap_servers=self.config.kafka.bootstrap_servers,
                    # La clave se serializará como string
                    key_serializer=lambda k: k.encode('utf-8') if k else None,
                    value_serializer=lambda v: json.dumps(v).encode('utf-8'),
                    api_version=(2, 0, 2),
                    retries=3,
                    request_timeout_ms=30000
                )
                # Test de conexión
                future = self.producer.send(self.config.kafka.topic_name, {'status': 'connected'})
                future.get(timeout=10)
                logger.info("✅ Conectado a Kafka exitosamente!")
                return
            except Exception as e:
                logger.error(f"❌ Intento {i+1} falló: {e}")
                if i < max_retries - 1:
                    import time
                    time.sleep(5)
        
        raise Exception("🚨 No se pudo conectar a Kafka")
    
    def publish(self, topic: str, message: Dict[str, Any], key: Optional[str] = None) -> bool:
        try:
            if self.producer:
                self.producer.send(topic, key=key, value=message)
                return True
        except KafkaError as e:
            logger.error(f"❌ Error enviando mensaje a Kafka: {e}")
        return False
    
    def flush(self) -> None:
        if self.producer:
            self.producer.flush()