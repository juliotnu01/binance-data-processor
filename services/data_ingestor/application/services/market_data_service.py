import asyncio
import json
import logging
import time
from typing import List, Dict, Any

from config.settings import Config
from domain.entities import MarketData
from application.use_cases.fetch_trading_pairs import FetchTradingPairsUseCase
from application.use_cases.fetch_historical_data import FetchHistoricalDataUseCase
from application.use_cases.stream_market_data import StreamMarketDataUseCase
from infrastructure.binance_client import BinanceClient
from infrastructure.kafka_producer import KafkaMessagePublisher
# Eliminamos la importación de NATS
# from infrastructure.nats_publisher import NATSPublisher
from infrastructure.websocket_manager import WebSocketManager

logger = logging.getLogger(__name__)

class MarketDataService:
    def __init__(self, config: Config):
        self.config = config
        self.binance_client = BinanceClient(config)
        self.kafka_publisher = KafkaMessagePublisher(config)
        # Eliminamos el publicador de NATS
        # self.nats_publisher = NATSPublisher(config)
        self.market_data = MarketData()
        self.websocket_manager = WebSocketManager(config, self.market_data)
        self.stream_use_case = StreamMarketDataUseCase(self.market_data, self.kafka_publisher) # <-- Pasamos el publisher
        
        # Registramos el manejador para klines
        self.websocket_manager.register_handler('kline_1h', self.stream_use_case.handle_kline_message)
    
    async def initialize(self) -> None:
        """Inicializar el servicio"""
        """Publica el estado de conexión."""
        status_message = {
            'event_type': 'connection_status',
            'status': 'connected',
            'timestamp': int(time.time() * 1000)
        }
        
        # Publicar en el tópico de estado
        self.kafka_publisher.publish(self.config.kafka.topic_connection_status, status_message)
        logger.info(f"🎯 Estado de conexión publicado en {self.config.kafka.topic_connection_status}")
        
        # Ya no nos conectamos a NATS
        # await self.nats_publisher.connect()
        
        # Obtener pares de trading
        fetch_pairs_use_case = FetchTradingPairsUseCase(
            self.binance_client, 
            self.config.app.max_leverage
        )
        trading_pairs = fetch_pairs_use_case.execute(self.config.app.max_trading_pairs)
        self.market_data.pairs = trading_pairs
        
        # Publicar datos iniciales solo en Kafka
        await self._publish_initial_data()
        
        # Obtener datos históricos y enviarlos a Kafka vela por vela
        await self._fetch_and_publish_historical_data()
        
        # Iniciar streams en tiempo real
        symbols = [pair.symbol for pair in trading_pairs]
        self.websocket_manager.start_all_streams(symbols)
        
        logger.info("🎯 Servicio de ingesta de datos en tiempo real iniciado.")
    
    async def _publish_initial_data(self) -> None:
        """Publicar datos iniciales de los pares en Kafka (sin clave, es un mensaje global)"""
        initial_data = {
            'event_type': 'initial_trading_pairs',
            'trading_pairs': [
                {
                    'symbol': pair.symbol,
                    'base_asset': pair.base_asset,
                    'quote_asset': pair.quote_asset,
                    'max_leverage': pair.max_leverage
                }
                for pair in self.market_data.pairs
            ],
            'total_pairs': len(self.market_data.pairs),
            'timestamp': int(time.time() * 1000)
        }
        
        # Publicar solo en Kafka sin clave, ya que es un mensaje de configuración global
        self.kafka_publisher.publish(self.config.kafka.topic_name, initial_data)
        logger.info(f"🎯 Datos iniciales de pares publicados en Kafka para {len(self.market_data.pairs)} pares")
    
    async def _fetch_and_publish_historical_data(self) -> None:
        """Obtiene datos históricos y los publica en Kafka vela por vela, con clave de símbolo."""
        logger.info("🔄 Iniciando carga y publicación de datos históricos...")
        
        fetch_historical_use_case = FetchHistoricalDataUseCase(self.binance_client)
        fetch_historical_use_case.execute(
            trading_pairs=self.market_data.pairs,
            interval=self.config.app.klines_interval,
            limit=self.config.app.klines_limit,
            batch_size=self.config.app.batch_size,
            batch_delay=self.config.app.batch_delay
        )
        
        for pair in self.market_data.pairs:  
            if not pair.candles:  
                continue  
            
            logger.info(f"📦 Publicando {len(pair.candles)} velas históricas para {pair.symbol}...")  
            for candle in pair.candles:  
                candle_event = {  
                    'event_type': 'historical_kline',  
                    'symbol': pair.symbol,  
                    'is_closed': True,  
                    'time': candle.time,  
                    'open': candle.open,  
                    'high': candle.high,  
                    'low': candle.low,  
                    'close': candle.close,  
                    'volume': candle.volume,  
                    'quote_volume': candle.quote_volume,  
                    'trades': candle.trades,  
                    'taker_buy_volume': candle.taker_buy_volume,  
                    'taker_buy_quote_volume': candle.taker_buy_quote_volume,  
                    'timestamp': candle.close_time  
                }  
                
                # MOVER ESTE BLOQUE DENTRO DEL LOOP (4 espacios más de indentación)  
                self.kafka_publisher.publish(    
                    self.config.kafka.topic_historical_klines,     
                    candle_event,     
                    key=pair.symbol    
                )  
        
        # Publicar evento de finalización (FUERA de ambos loops)  
        end_event = {  
            'event_type': 'historical_data_loaded',  
            'total_pairs_with_candles': len([p for p in self.market_data.pairs if p.candles]),  
            'timestamp': int(time.time() * 1000)  
        }  
        self.kafka_publisher.publish(self.config.kafka.topic_historical_klines, end_event)  
        logger.info("✅ Carga y publicación de datos históricos completada.")

    # Eliminamos los métodos de publicación periódica y de NATS
    # def _start_periodic_publishing(self) -> None ...
    # async def _publish_market_data(self) -> None ...
    
    async def shutdown(self) -> None:
        """Apagar el servicio"""
        logger.info("🛑 Apagando Market Data Service...")
        
        # Cerrar WebSockets
        self.websocket_manager.stop()
        
        # Cerrar conexión de Kafka
        self.kafka_publisher.flush()
        
        # Ya no hay que cerrar NATS
        # await self.nats_publisher.close()
        
        logger.info("👋 Market Data Service apagado correctamente")