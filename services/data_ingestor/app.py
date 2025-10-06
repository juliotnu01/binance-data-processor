import os
import json
import time
import threading
import logging
import requests
from concurrent.futures import ThreadPoolExecutor, as_completed
from kafka import KafkaProducer
import websocket

# Configuración de logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Configuración desde variables de entorno
KAFKA_BOOTSTRAP_SERVERS = os.environ.get('KAFKA_BOOTSTRAP_SERVERS', 'kafka:29092')
TOPIC_NAME = os.environ.get('TOPIC_NAME', 'binance_public_trades')
BINANCE_WS_URL = os.environ.get('BINANCE_WS_URL', 'wss://fstream.binance.com/ws/')
BINANCE_API_BASE = os.environ.get('BINANCE_API_BASE', 'https://fapi.binance.com')

# Configuración de agregación
AGGREGATION_INTERVAL = int(os.environ.get('AGGREGATION_INTERVAL', '5'))  # segundos
BATCH_SIZE = int(os.environ.get('BATCH_SIZE', '10'))
BATCH_DELAY = int(os.environ.get('BATCH_DELAY', '1'))  # segundos

class BinanceDataFetcher:
    """Clase para obtener información de pares de trading de Binance (solo endpoints públicos)"""
    
    def __init__(self):
        self.api_base = BINANCE_API_BASE
        
    def get_exchange_info(self):
        """Obtiene información del exchange (endpoint público)"""
        try:
            response = requests.get(f"{self.api_base}/fapi/v1/exchangeInfo", timeout=10)
            response.raise_for_status()
            return response.json()
        except Exception as e:
            logger.error(f"❌ Error obteniendo exchange info: {e}")
            return None
    
    def get_ticker_24hr(self, symbol=None):
        """Obtiene estadísticas de 24hr para un símbolo o todos (endpoint público)"""
        try:
            url = f"{self.api_base}/fapi/v1/ticker/24hr"
            if symbol:
                url += f"?symbol={symbol}"
            
            response = requests.get(url, timeout=10)
            response.raise_for_status()
            return response.json()
        except Exception as e:
            logger.error(f"❌ Error obteniendo ticker 24hr: {e}")
            return None
    
    def get_klines(self, symbol, interval="1h", limit=1000):
        """Obtiene velas para un símbolo específico (endpoint público)"""
        try:
            params = {
                'symbol': symbol,
                'interval': interval,
                'limit': limit
            }
            response = requests.get(f"{self.api_base}/fapi/v1/klines", params=params, timeout=10)
            response.raise_for_status()
            return response.json()
        except Exception as e:
            logger.error(f"❌ Error obteniendo klines para {symbol}: {e}")
            return None

class TradingPairManager:
    """Gestiona los pares de trading y sus datos históricos"""
    
    def __init__(self):
        self.fetcher = BinanceDataFetcher()
        self.trading_pairs = []
        
    def fetch_trading_pairs(self):
        """Obtiene todos los pares de trading USDT (sin filtro de leverage)"""
        logger.info("🔄 Obteniendo pares de trading USDT de Binance Futures...")
        
        exchange_info = self.fetcher.get_exchange_info()
        
        if not exchange_info:
            logger.error("❌ No se pudo obtener información del exchange")
            return []
        
        symbols = exchange_info.get('symbols', [])
        
        # Filtrar pares USDT PERPETUAL en trading
        usdt_pairs = [
            symbol for symbol in symbols
            if (symbol.get('quoteAsset') == 'USDT' and
                symbol.get('status') == 'TRADING' and
                symbol.get('contractType') == 'PERPETUAL')
        ]
        
        logger.info(f"📊 Encontrados {len(usdt_pairs)} pares USDT PERPETUAL en trading")
        
        # Obtener volumen de 24hr para cada par
        ticker_24hr = self.fetcher.get_ticker_24hr()
        ticker_dict = {ticker['symbol']: ticker for ticker in ticker_24hr} if ticker_24hr else {}
        
        # Crear lista de pares con información básica
        filtered_pairs = []
        for symbol in usdt_pairs:
            symbol_data = ticker_dict.get(symbol['symbol'], {})
            filtered_pairs.append({
                'symbol': symbol['symbol'],
                'baseAsset': symbol.get('baseAsset', ''),
                'quoteAsset': symbol.get('quoteAsset', ''),
                'filters': symbol.get('filters', []),
                'volume_24h': float(symbol_data.get('volume', 0)),
                'quoteVolume_24h': float(symbol_data.get('quoteVolume', 0)),
                'count_24h': int(symbol_data.get('count', 0))
            })
        
        # Ordenar por volumen (mayor a menor)
        filtered_pairs.sort(key=lambda x: x['quoteVolume_24h'], reverse=True)
        
        # Limitar a los top 50 pares por volumen (opcional, quitar si quieres todos)
        top_pairs = filtered_pairs[:50]
        
        logger.info(f"🎯 Seleccionados {len(top_pairs)} pares (top por volumen)")
        for i, pair in enumerate(top_pairs[:10]):  # Mostrar top 10
            logger.info(f"   {i+1}. {pair['symbol']} - Volumen 24h: ${pair['quoteVolume_24h']:,.0f}")
        
        self.trading_pairs = top_pairs
        return top_pairs
    
    def fetch_all_klines(self, interval="1h", limit=1000):
        """Obtiene velas para todos los pares de trading en lotes"""
        if not self.trading_pairs:
            logger.warning("⚠️ No hay pares de trading para obtener velas")
            return {}
            
        logger.info(f"🔄 Obteniendo {limit} velas {interval} para {len(self.trading_pairs)} pares...")
        
        symbols = [pair['symbol'] for pair in self.trading_pairs]
        results = {}
        
        # Procesar en lotes
        for i in range(0, len(symbols), BATCH_SIZE):
            batch = symbols[i:i + BATCH_SIZE]
            logger.info(f"📦 Procesando lote {i//BATCH_SIZE + 1}/{(len(symbols)-1)//BATCH_SIZE + 1}")
            
            with ThreadPoolExecutor(max_workers=BATCH_SIZE) as executor:
                future_to_symbol = {
                    executor.submit(self.fetcher.get_klines, symbol, interval, limit): symbol 
                    for symbol in batch
                }
                
                for future in as_completed(future_to_symbol):
                    symbol = future_to_symbol[future]
                    try:
                        klines_data = future.result()
                        if klines_data:
                            processed_candles = self._process_klines(klines_data)
                            results[symbol] = processed_candles
                            logger.info(f"✅ Velas obtenidas para {symbol}: {len(processed_candles)} candles")
                        else:
                            logger.warning(f"⚠️ No se pudieron obtener velas para {symbol}")
                    except Exception as e:
                        logger.error(f"❌ Error procesando {symbol}: {e}")
            
            # Esperar entre lotes
            if i + BATCH_SIZE < len(symbols):
                logger.info(f"⏳ Esperando {BATCH_DELAY} segundos...")
                time.sleep(BATCH_DELAY)
        
        # Actualizar pares con velas
        candles_loaded = 0
        for pair in self.trading_pairs:
            pair['candles'] = results.get(pair['symbol'], [])
            pair['candles_count'] = len(pair['candles'])
            if pair['candles']:
                candles_loaded += 1
        
        logger.info(f"🎯 Velas cargadas para {candles_loaded}/{len(self.trading_pairs)} pares")
        return results
    
    def _process_klines(self, klines_data):
        """Procesa los datos de klines a un formato más manejable"""
        processed = []
        for candle in klines_data:
            processed.append({
                'time': candle[0],  # Timestamp de apertura
                'human_time': self._timestamp_to_human(candle[0]),
                'open': float(candle[1]),
                'high': float(candle[2]),
                'low': float(candle[3]),
                'close': float(candle[4]),
                'volume': float(candle[5]),
                'close_time': candle[6],
                'quote_asset_volume': float(candle[7]),
                'trades': candle[8],
                'taker_buy_base_volume': float(candle[9]),
                'taker_buy_quote_volume': float(candle[10])
            })
        return processed
    
    def _timestamp_to_human(self, timestamp):
        """Convierte timestamp a formato legible"""
        return time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(timestamp / 1000))

class KafkaManager:
    """Gestiona la conexión y envío de datos a Kafka"""
    
    def __init__(self):
        self.producer = None
    
    def initialize_producer(self):
        """Inicializa el productor de Kafka con reintentos"""
        max_retries = 10
        for i in range(max_retries):
            try:
                logger.info(f"🔄 Intento {i+1}/{max_retries} de conectar a Kafka...")
                self.producer = KafkaProducer(
                    bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
                    value_serializer=lambda v: json.dumps(v).encode('utf-8'),
                    api_version=(2, 0, 2),
                    retries=3,
                    request_timeout_ms=30000
                )
                # Test de conexión
                future = self.producer.send(TOPIC_NAME, {'status': 'ingestor_started'})
                future.get(timeout=10)
                logger.info("✅ Conectado a Kafka exitosamente!")
                return True
            except Exception as e:
                logger.error(f"❌ Intento {i+1} falló: {e}")
                if i < max_retries - 1:
                    time.sleep(5)
        
        logger.error("🚨 No se pudo conectar a Kafka")
        return False
    
    def send_message(self, topic, message):
        """Envía un mensaje a Kafka"""
        try:
            if self.producer:
                self.producer.send(topic, value=message)
                return True
        except Exception as e:
            logger.error(f"❌ Error enviando mensaje a Kafka: {e}")
        return False
    
    def flush(self):
        """Fuerza el envío de mensajes pendientes"""
        if self.producer:
            self.producer.flush()

class WebSocketManager:
    """Gestiona múltiples conexiones WebSocket a Binance"""
    
    def __init__(self, kafka_manager, pair_manager):
        self.kafka = kafka_manager
        self.pair_manager = pair_manager
        self.websockets = {}
        self.running = True
        
        # Estado para agregación
        self.aggregation_data = {}  # symbol -> datos del tick
        self.aggregation_lock = threading.Lock()
    
    def start_all_streams(self):
        """Inicia streams para todos los pares de trading"""
        if not self.pair_manager.trading_pairs:
            logger.error("❌ No hay pares de trading para iniciar streams")
            return
            
        symbols = [pair['symbol'].lower() for pair in self.pair_manager.trading_pairs]
        logger.info(f"🎯 Iniciando {len(symbols)} streams WebSocket...")
        
        # Agrupar símbolos en streams combinados (máximo 200 por stream)
        streams = []
        for i in range(0, len(symbols), 200):
            batch = symbols[i:i + 200]
            stream_name = "/".join([f"{symbol}@trade" for symbol in batch])
            streams.append(stream_name)
        
        # Iniciar streams
        for stream in streams:
            self.start_websocket(stream)
        
        # Iniciar agregación
        self.start_aggregation()
        
        logger.info(f"🎯 {len(streams)} streams WebSocket iniciados")
    
    def start_websocket(self, stream_name):
        """Inicia una conexión WebSocket para un conjunto de streams"""
        ws_url = f"{BINANCE_WS_URL}{stream_name}"
        
        def on_message(ws, message):
            self.handle_trade_message(message)
        
        def on_error(ws, error):
            logger.error(f"❌ WebSocket error para {stream_name}: {error}")
        
        def on_close(ws, close_status_code, close_msg):
            logger.warning(f"🔌 WebSocket cerrado: {stream_name}")
            if self.running:
                logger.info(f"🔄 Reconectando {stream_name} en 5 segundos...")
                time.sleep(5)
                self.start_websocket(stream_name)
        
        def on_open(ws):
            logger.info(f"✅ WebSocket conectado: {stream_name}")
        
        ws = websocket.WebSocketApp(
            ws_url,
            on_open=on_open,
            on_message=on_message,
            on_error=on_error,
            on_close=on_close
        )
        
        # Ejecutar en hilo separado
        thread = threading.Thread(
            target=ws.run_forever,
            name=f"WebSocket-{stream_name[:20]}",
            daemon=True
        )
        thread.start()
        
        self.websockets[stream_name] = ws
    
    def handle_trade_message(self, message):
        """Procesa un mensaje de trade individual"""
        try:
            data = json.loads(message)
            if not isinstance(data, dict) or 's' not in data:
                return
            
            symbol = data['s']
            price = float(data['p'])
            quantity = float(data['q'])
            timestamp = data.get('T') or data.get('E')
            
            # Actualizar datos de agregación
            with self.aggregation_lock:
                if symbol not in self.aggregation_data:
                    self.aggregation_data[symbol] = {
                        'symbol': symbol,
                        'open': price,
                        'high': price,
                        'low': price,
                        'close': price,
                        'volume': 0.0,
                        'trades': 0,
                        'timestamp': timestamp
                    }
                
                tick_data = self.aggregation_data[symbol]
                
                # Actualizar OHLCV
                if tick_data['trades'] == 0:
                    tick_data['open'] = price
                else:
                    if price > tick_data['high']:
                        tick_data['high'] = price
                    if price < tick_data['low']:
                        tick_data['low'] = price
                
                tick_data['close'] = price
                tick_data['volume'] += price * quantity
                tick_data['trades'] += 1
                tick_data['timestamp'] = timestamp
            
            # Enviar trade individual
            trade_message = {
                'symbol': symbol,
                'price': price,
                'quantity': quantity,
                'timestamp': timestamp,
                'event_type': 'trade',
                'is_maker': data.get('m', False),
                'trade_id': data.get('t')
            }
            self.kafka.send_message(TOPIC_NAME, trade_message)
            
        except Exception as e:
            logger.error(f"❌ Error procesando mensaje trade: {e}")
    
    def start_aggregation(self):
        """Inicia el proceso de agregación periódica"""
        def aggregate_loop():
            while self.running:
                time.sleep(AGGREGATION_INTERVAL)
                self.send_aggregated_data()
        
        aggregation_thread = threading.Thread(
            target=aggregate_loop,
            name="AggregationThread",
            daemon=True
        )
        aggregation_thread.start()
        logger.info(f"🔄 Agregación iniciada cada {AGGREGATION_INTERVAL} segundos")
    
    def send_aggregated_data(self):
        """Envía datos agregados a Kafka"""
        with self.aggregation_lock:
            if not self.aggregation_data:
                return
            
            current_data = self.aggregation_data.copy()
            self.aggregation_data = {}  # Reset para siguiente intervalo
            
            for symbol, tick_data in current_data.items():
                if tick_data['trades'] > 0:
                    # Preparar mensaje agregado
                    aggregated_message = {
                        'symbol': tick_data['symbol'],
                        'timestamp': tick_data['timestamp'],
                        'open': tick_data['open'],
                        'high': tick_data['high'],
                        'low': tick_data['low'],
                        'close': tick_data['close'],
                        'volume': round(tick_data['volume'], 4),
                        'trades': tick_data['trades'],
                        'event_type': 'aggregated_tick',
                        'aggregation_interval': AGGREGATION_INTERVAL
                    }
                    
                    logger.info(f"📊 Tick {symbol}: O:{aggregated_message['open']} H:{aggregated_message['high']} L:{aggregated_message['low']} C:{aggregated_message['close']} V:{aggregated_message['volume']} T:{aggregated_message['trades']}")
                    
                    # Enviar a Kafka
                    self.kafka.send_message(TOPIC_NAME, aggregated_message)
            
            self.kafka.flush()
    
    def stop(self):
        """Detiene todos los WebSockets"""
        self.running = False
        for ws in self.websockets.values():
            try:
                ws.close()
            except:
                pass

def main():
    """Función principal"""
    logger.info("🚀 Iniciando Binance Data Ingestor Multi-Pair")
    logger.info(f"📍 Kafka: {KAFKA_BOOTSTRAP_SERVERS}")
    logger.info(f"📊 Tópico: {TOPIC_NAME}")
    logger.info(f"⏱️  Agregación: {AGGREGATION_INTERVAL}s")
    
    try:
        # 1. Inicializar managers
        pair_manager = TradingPairManager()
        kafka_manager = KafkaManager()
        
        if not kafka_manager.initialize_producer():
            logger.error("🚨 No se pudo inicializar Kafka, terminando...")
            return
        
        # 2. Obtener pares de trading
        pairs = pair_manager.fetch_trading_pairs()
        if not pairs:
            logger.error("🚨 No se encontraron pares de trading, terminando...")
            return
        
        # 3. Enviar datos iniciales a Kafka
        initial_data = {
            'event_type': 'initial_data',
            'trading_pairs': pair_manager.trading_pairs,
            'total_pairs': len(pair_manager.trading_pairs),
            'timestamp': int(time.time() * 1000)
        }
        kafka_manager.send_message(TOPIC_NAME, initial_data)
        kafka_manager.flush()
        
        logger.info(f"🎯 Datos iniciales enviados para {len(pair_manager.trading_pairs)} pares")
        
        # 4. Iniciar streams en tiempo real
        ws_manager = WebSocketManager(kafka_manager, pair_manager)
        ws_manager.start_all_streams()
        
        # 5. Obtener datos históricos en segundo plano
        def fetch_historical_data():
            time.sleep(10)  # Esperar a que los streams estén funcionando
            pair_manager.fetch_all_klines(interval="1h", limit=1000)
            
            # Enviar datos históricos a Kafka
            historical_data = {
                'event_type': 'historical_data_loaded',
                'total_pairs_with_candles': len([p for p in pair_manager.trading_pairs if p.get('candles')]),
                'timestamp': int(time.time() * 1000)
            }
            kafka_manager.send_message(TOPIC_NAME, historical_data)
            kafka_manager.flush()
        
        historical_thread = threading.Thread(target=fetch_historical_data, daemon=True)
        historical_thread.start()
        
        logger.info("🎯 Data Ingestor funcionando. Presiona Ctrl+C para detener.")
        
        # Mantener el programa corriendo
        while True:
            time.sleep(1)
            
    except KeyboardInterrupt:
        logger.info("🛑 Deteniendo Data Ingestor...")
        if 'ws_manager' in locals():
            ws_manager.stop()
        logger.info("👋 Data Ingestor detenido correctamente")
    except Exception as e:
        logger.error(f"🚨 Error crítico: {e}")
        raise

if __name__ == "__main__":
    main()