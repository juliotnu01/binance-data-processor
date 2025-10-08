import os
from dataclasses import dataclass

@dataclass
class KafkaConfig:
    consumer_group_id: str = os.getenv('CONSUMER_GROUP_ID', 'rsi-calculator-group')
    # Tópicos de entrada
    bootstrap_servers: str = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'kafka:29092')
    input_topic: str = os.getenv('INPUT_TOPIC', 'binance_public_trades')
    # Tópico de salida
    topic_connection_status: str = os.getenv('TOPIC_CONNECTION_STATUS', 'connection_status')
    topic_historical_klines: str = os.getenv('TOPIC_HISTORICAL_KLINES', 'historical_klines')
    topic_realtime_klines: str = os.getenv('TOPIC_REALTIME_KLINES', 'realtime_klines')
    output_topic: str = os.getenv('OUTPUT_TOPIC', 'rsi_signals')
@dataclass
class RSIConfig:
    period: int = int(os.getenv('RSI_PERIOD', '14'))

@dataclass
class Config:
    kafka: KafkaConfig = KafkaConfig()
    rsi: RSIConfig = RSIConfig()

config = Config()