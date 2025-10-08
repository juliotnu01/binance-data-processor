import json
import logging
from typing import Callable
from kafka import KafkaConsumer
from kafka.errors import KafkaError
from config.settings import Config

logger = logging.getLogger(__name__)

class KafkaRSIConsumer:
    """
    Un consumidor de Kafka robusto y configurable para el servicio de RSI.
    Se encarga de leer mensajes de un tópico de entrada y pasarlos a un manejador.
    """
    def __init__(self, config: Config, message_handler: Callable):
        """
        Inicializa el consumidor de Kafka.

        Args:
            config (Config): El objeto de configuración de la aplicación.
            message_handler (Callable): Una función que será llamada para cada mensaje consumido.
            Debe aceptar un objeto mensaje de Kafka como argumento.
        """
        self.config = config
        self.message_handler = message_handler
        self.running = True
        
        logger.info(f"🔌 Conectando consumidor de Kafka al tópico '{self.config.kafka.input_topic}'...")
        self.consumer = KafkaConsumer(
            self.config.kafka.input_topic,
            bootstrap_servers=self.config.kafka.bootstrap_servers,  # Ejemplo: "kafka:29092"
            group_id=self.config.kafka.consumer_group_id,
            value_deserializer=lambda v: json.loads(v.decode('utf-8')),
            key_deserializer=lambda k: k.decode('utf-8') if k else None,
            auto_offset_reset='earliest',
            enable_auto_commit=False
        )
        logger.info("✅ Consumidor de Kafka inicializado correctamente.")

    def run(self):
        """
        Inicia el bucle principal del consumidor para procesar mensajes.
        """
        logger.info("🔄 Iniciando bucle de consumo de mensajes para RSI...")
        try:
            while self.running:
                message_batch = self.consumer.poll(timeout_ms=1000)
                for topic_partition, messages in message_batch.items():
                    for message in messages:
                        try:
                            self.message_handler(message)
                            self.consumer.commit()
                            logger.debug(f"✅ Mensaje procesado y offset hecho commit para {message.key}")
                        except Exception as e:
                            logger.error(f"❌ Error procesando mensaje de {message.key}. Offset: {message.offset}. Error: {e}")
                            logger.error(f"Contenido del mensaje fallido: {message.value}")
                            # No hacemos commit para reintento posterior
        except KafkaError as e:
            logger.error(f"🚨 Error crítico en el consumidor de Kafka: {e}")
        finally:
            self.consumer.close()
            logger.info("👋 Consumidor de Kafka cerrado.")

    def stop(self):
        """
        Detiene el bucle del consumidor de forma graceful.
        """
        logger.info("🛑 Señal de parada recibida. Deteniendo el consumidor de Kafka...")
        self.running = False
