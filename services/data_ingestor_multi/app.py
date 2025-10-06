import os
import json
import time
import threading
import requests
from kafka import KafkaProducer
import websocket

KAFKA_BOOTSTRAP_SERVERS = os.environ.get('KAFKA_BOOTSTRAP_SERVERS', 'kafka:29092')
TOPIC_NAME = os.environ.get('TOPIC_NAME', 'binance_public_trades')
BINANCE_WS_URL = os.environ.get('BINANCE_WS_URL', 'wss://fstream.binance.com/ws/')
SYMBOL = os.environ.get('SYMBOL')
AGGREGATION_INTERVAL = 1
BASE_URL = os.environ.get('BASE_URL', 'https://fapi.binance.com')

if not SYMBOL:
    raise ValueError("La variable de entorno SYMBOL no está definida.")

print(f"🚀 Iniciando Data Ingestor para el símbolo: {SYMBOL}")

producer = KafkaProducer(
    bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
    value_serializer=lambda v: json.dumps(v).encode('utf-8'),
    api_version=(2, 0, 2)
)

def fetch_and_send_historical_data():
    print(f"📚 Obteniendo 1000 velas históricas para {SYMBOL}...")
    try:
        params = {'symbol': SYMBOL, 'interval': '1m', 'limit': 1000}
        response = requests.get(f"{BASE_URL}/fapi/v1/klines", params=params)
        response.raise_for_status()
        
        for candle in response.json():
            historical_tick = {
                'symbol': SYMBOL, 'timestamp': int(candle[0]), 'open': float(candle[1]),
                'high': float(candle[2]), 'low': float(candle[3]), 'close': float(candle[4]),
                'volume': float(candle[5]), 'trades': 0, 'source': 'historical'
            }
            producer.send(TOPIC_NAME, key=SYMBOL, value=historical_tick)
        
        producer.flush()
        print(f"✅ Histórico de {SYMBOL} enviado a Kafka.")
    except Exception as e:
        print(f"❌ Error obteniendo/enviando histórico para {SYMBOL}: {e}")

current_tick = {'symbol': None, 'open': None, 'high': None, 'low': None, 'close': None, 'volume': 0.0, 'trades': 0, 'timestamp': None}
tick_lock = threading.Lock()

def aggregate_and_send():
    global current_tick
    with tick_lock:
        if current_tick['trades'] > 0:
            message_to_send = {**current_tick, 'source': 'live'}
            producer.send(TOPIC_NAME, key=SYMBOL, value=message_to_send)
            producer.flush()
            print(f"📤 [LIVE] Enviando tick de {SYMBOL} a Kafka.")
        current_tick = {k: (0.0 if k in ['volume'] else 0 if k == 'trades' else None) for k in current_tick}
        current_tick['symbol'] = SYMBOL
    threading.Timer(AGGREGATION_INTERVAL, aggregate_and_send).start()

def on_message(ws, message):
    try:
        data = json.loads(message)
        if not isinstance(data, dict) or 'p' not in data or 'q' not in data: return
        price, quantity, timestamp = float(data['p']), float(data['q']), int(data['T'])
        with tick_lock:
            if current_tick['trades'] == 0:
                current_tick['open'], current_tick['high'], current_tick['low'] = price, price, price
                current_tick['timestamp'] = timestamp
            else:
                if price > current_tick['high']: current_tick['high'] = price
                if price < current_tick['low']: current_tick['low'] = price
            current_tick['close'] = price
            current_tick['volume'] += price * quantity
            current_tick['trades'] += 1
    except Exception as e: print(f"❌ Error procesando mensaje: {e}")

def on_error(ws, error): print(f"❌ WebSocket error: {error}")
def on_close(ws, close_status_code, close_msg):
    print(f"🔌 WebSocket cerrado para {SYMBOL}. Reconectando...")
    time.sleep(5)
    start_websocket()
def on_open(ws): print(f"✅ WebSocket conectado para {SYMBOL}")

def start_websocket():
    stream_name = f"{SYMBOL.lower()}@trade"
    ws_url = f"{BINANCE_WS_URL}{stream_name}"
    ws = websocket.WebSocketApp(ws_url, on_open=on_open, on_message=on_message, on_error=on_error, on_close=on_close)
    ws.run_forever()

if __name__ == "__main__":
    fetch_and_send_historical_data()
    aggregate_and_send()
    start_websocket()