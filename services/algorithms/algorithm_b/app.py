import os
import json
import pandas as pd
import numpy as np
from kafka import KafkaConsumer, KafkaProducer
import time

# Configuración desde variables de entorno
KAFKA_BOOTSTRAP_SERVERS = os.environ.get('KAFKA_BOOTSTRAP_SERVERS', 'kafka:29092')
INPUT_TOPIC = os.environ.get('INPUT_TOPIC', 'binance_public_trades')
OUTPUT_TOPIC = os.environ.get('OUTPUT_TOPIC', 'signals_momentum')
WINDOW_SIZE = 50  # Ahora son 50 ticks de 1 segundo = 50 segundos de datos

def create_kafka_consumer():
    max_retries = 10
    retry_count = 0
    while retry_count < max_retries:
        try:
            consumer = KafkaConsumer(
                INPUT_TOPIC,
                bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
                value_deserializer=lambda m: json.loads(m.decode('utf-8')),
                auto_offset_reset='latest',
                group_id='algorithm_b_momentum'
            )
            print(f"✅ Algoritmo B (Momentum) conectado a Kafka en {KAFKA_BOOTSTRAP_SERVERS}")
            return consumer
        except Exception as e:
            retry_count += 1
            print(f"⚠️ Intento {retry_count}/{max_retries} - Error conectando a Kafka: {e}")
            if retry_count < max_retries:
                print("Esperando 5 segundos antes de reintentar...")
                time.sleep(5)
    raise Exception(f"No se pudo conectar a Kafka después de {max_retries} intentos")

def create_kafka_producer():
    max_retries = 10
    retry_count = 0
    while retry_count < max_retries:
        try:
            producer = KafkaProducer(
                bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
                key_serializer=lambda k: k.encode('utf-8') if k else None,
                value_serializer=lambda v: json.dumps(v).encode('utf-8')
            )
            print(f"✅ Productor de Algoritmo B conectado a Kafka en {KAFKA_BOOTSTRAP_SERVERS}")
            return producer
        except Exception as e:
            retry_count += 1
            print(f"⚠️ Intento {retry_count}/{max_retries} - Error conectando productor a Kafka: {e}")
            if retry_count < max_retries:
                print("Esperando 5 segundos antes de reintentar...")
                time.sleep(5)
    raise Exception(f"No se pudo conectar el productor a Kafka después de {max_retries} intentos")

def to_native_json_type(value):
    """Convierte tipos de NumPy a tipos nativos de Python para JSON."""
    if isinstance(value, (np.integer, np.int64)):
        return int(value)
    if isinstance(value, (np.floating, np.float64)):
        return float(value)
    return value

def calculate_momentum_signals(data_window):
    """
    Calcula señales de momentum basadas en RSI y EMAs
    sobre una ventana de ticks OHLCV.
    """
    if len(data_window) < WINDOW_SIZE:
        return None

    try:
        df = pd.DataFrame(data_window)
        
        numeric_cols = ['open', 'high', 'low', 'close', 'volume']
        for col in numeric_cols:
            df[col] = pd.to_numeric(df[col], errors='coerce')
        
        df.dropna(subset=['close'], inplace=True)

        if len(df) < WINDOW_SIZE:
            return None

        # --- CÁLCULO DE INDICADORES ---
        # Usamos el precio de cierre ('close') para los cálculos
        
        # 1. RSI (Relative Strength Index)
        delta = df['close'].diff()
        gain = (delta.where(delta > 0, 0)).rolling(window=14).mean()
        loss = (-delta.where(delta < 0, 0)).rolling(window=14).mean()
        rs = gain / loss
        df['rsi'] = 100 - (100 / (1 + rs))

        # 2. EMAs (Exponential Moving Averages)
        df['ema_20'] = df['close'].ewm(span=20).mean()
        df['ema_50'] = df['close'].ewm(span=50).mean()

        # Obtener los valores más recientes
        last_row = df.iloc[-1]
        last_price = last_row['close']
        last_rsi = last_row['rsi']
        last_ema_20 = last_row['ema_20']
        last_ema_50 = last_row['ema_50']

        # --- LÓGICA DE SEÑAL ---
        signal = None
        if last_rsi < 30 and last_ema_20 > last_ema_50:
            signal = "BUY"  # Sobreventa con tendencia alcista
        elif last_rsi > 70 and last_ema_20 < last_ema_50:
            signal = "SELL"  # Sobrecompra con tendencia bajista

        if signal:
            return {
                "symbol": last_row['symbol'],
                "price": to_native_json_type(last_price),
                "signal": signal,
                "algorithm": "momentum",
                "timestamp": to_native_json_type(last_row['timestamp']),
                "rsi": to_native_json_type(last_rsi),
                "ema_20": to_native_json_type(last_ema_20),
                "ema_50": to_native_json_type(last_ema_50),
                "window_size": WINDOW_SIZE
            }
    except Exception as e:
        print(f"❌ Error en Algoritmo B calculando señales: {e}")

    return None

def process_messages(consumer, producer):
    print(f"🎯 Iniciando procesamiento de mensajes para Algoritmo B (Momentum)...")
    print(f"📥 Consumiendo de: {INPUT_TOPIC}")
    print(f"📤 Produciendo a: {OUTPUT_TOPIC}")
    print(f"📊 Ventana de cálculo: {WINDOW_SIZE} ticks de 1 segundo.")

    data_window = []

    for message in consumer:
        try:
            data_window.append(message.value)

            if len(data_window) > WINDOW_SIZE:
                data_window.pop(0)

            signal = calculate_momentum_signals(data_window)

            if signal:
                producer.send(OUTPUT_TOPIC, 
                                            key=f"{signal['symbol']}_{int(time.time())}".encode('utf-8'), 
                                            value=signal)
                producer.flush()
                print(f"✅ [Algoritmo B] Señal generada: {signal['symbol']} - {signal['signal']} @ ${signal['price']}")
            # else:
            #     print(f"📊 [Algoritmo B] Datos procesados (ventana: {len(data_window)}/{WINDOW_SIZE}) - Sin señal")

        except Exception as e:
            print(f"❌ [Algoritmo B] Error procesando mensaje: {e}")

if __name__ == "__main__":
    print(f"🚀 Iniciando Algoritmo B (Momentum)")
    print(f"📍 Bootstrap servers: {KAFKA_BOOTSTRAP_SERVERS}")
    
    consumer = create_kafka_consumer()
    producer = create_kafka_producer()
    
    process_messages(consumer, producer)