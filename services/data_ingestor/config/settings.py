import os
from dataclasses import dataclass
from typing import Optional

@dataclass
class KafkaConfig:
    bootstrap_servers: str = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'kafka:29092')
    topic_name: str = os.getenv('TOPIC_NAME', 'binance_public_trades')

@dataclass
class BinanceConfig:
    ws_url: str = os.getenv('BINANCE_WS_URL', 'wss://fstream.binance.com')
    api_base: str = os.getenv('BINANCE_API_BASE', 'https://fapi.binance.com')
    api_key: str = os.getenv('BINANCE_API_KEY', '')
    api_secret: str = os.getenv('BINANCE_API_SECRET', '')

@dataclass
class NATSConfig:
    url: str = os.getenv('NATS_URL', 'nats://nats:4222')
    subject_prefix: str = os.getenv('NATS_SUBJECT_PREFIX', 'binance')

@dataclass
class AppConfig:
    aggregation_interval: int = int(os.getenv('AGGREGATION_INTERVAL', '5'))
    batch_size: int = int(os.getenv('BATCH_SIZE', '10'))
    batch_delay: int = int(os.getenv('BATCH_DELAY', '1'))
    max_trading_pairs: int = int(os.getenv('MAX_TRADING_PAIRS', '50'))
    klines_interval: str = os.getenv('KLINES_INTERVAL', '1h')
    klines_limit: int = int(os.getenv('KLINES_LIMIT', '1000'))
    max_leverage: int = int(os.getenv('MAX_LEVERAGE', '75'))

@dataclass
class Config:
    kafka: KafkaConfig = KafkaConfig()
    binance: BinanceConfig = BinanceConfig()
    nats: NATSConfig = NATSConfig()
    app: AppConfig = AppConfig()

config = Config()