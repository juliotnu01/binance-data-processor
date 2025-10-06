import os
import json
import pandas as pd
import numpy as np
from kafka import KafkaConsumer, KafkaProducer
import time

# Configuración desde variables de entorno
KAFKA_BOOTSTRAP_SERVERS = os.environ.get('KAFKA_BOOTSTRAP_SERVERS', 'kafka:29092')
INPUT_TOPIC = os.environ.get('INPUT_TOPIC', 'binance_public_trades')
OUTPUT_TOPIC = os.environ.get('OUTPUT_TOPIC', 'signals_scalping')
WINDOW_SIZE = 20  # Ahora son 20 ticks de 1 segundo = 20 segundos de datos

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
                group_id='algorithm_a_scalping'
            )
            print(f"✅ Algoritmo A (Scalping) conectado a Kafka en {KAFKA_BOOTSTRAP_SERVERS}")
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
            print(f"✅ Productor de Algoritmo A conectado a Kafka en {KAFKA_BOOTSTRAP_SERVERS}")
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

def calculate_scalping_signals(data_window):
    """
    Calcula señales de scalping basadas en Bandas de Bollinger
    sobre una ventana de ticks OHLCV.
    """
    if len(data_window) < WINDOW_SIZE:
        return None

    try:
        # Convertimos la lista de diccionarios a un DataFrame de Pandas
        df = pd.DataFrame(data_window)
        
        # Nos aseguramos de que las columnas numéricas lo sean
        numeric_cols = ['open', 'high', 'low', 'close', 'volume']
        for col in numeric_cols:
            df[col] = pd.to_numeric(df[col], errors='coerce')
        
        df.dropna(subset=['close'], inplace=True) # Eliminamos filas sin precio de cierre

        if len(df) < WINDOW_SIZE:
            return None

        # --- CÁLCULO DE INDICADORES ---
        # Usamos el precio de cierre ('close') para los cálculos
        df['sma'] = df['close'].rolling(window=WINDOW_SIZE).mean()
        df['std'] = df['close'].rolling(window=WINDOW_SIZE).std()
        df['upper_band'] = df['sma'] + (df['std'] * 2)
        df['lower_band'] = df['sma'] - (df['std'] * 2)

        # Obtener los valores más recientes
        last_row = df.iloc[-1]
        last_price = last_row['close']
        last_sma = last_row['sma']
        last_upper = last_row['upper_band']
        last_lower = last_row['lower_band']

        # --- LÓGICA DE SEÑAL ---
        signal = None
        if last_price > last_upper:
            signal = "SELL"  # Sobrecompra
        elif last_price < last_lower:
            signal = "BUY"   # Sobreventa

        if signal:
            return {
                "symbol": last_row['symbol'],
                "price": to_native_json_type(last_price),
                "signal": signal,
                "algorithm": "scalping",
                "timestamp": to_native_json_type(last_row['timestamp']),
                "sma": to_native_json_type(last_sma),
                "upper_band": to_native_json_type(last_upper),
                "lower_band": to_native_json_type(last_lower),
                "window_size": WINDOW_SIZE
            }
    except Exception as e:
        print(f"❌ Error en Algoritmo A calculando señales: {e}")

    return None

def process_messages(consumer, producer):
    print(f"🎯 Iniciando procesamiento de mensajes para Algoritmo A (Scalping)...")
    print(f"📥 Consumiendo de: {INPUT_TOPIC}")
    print(f"📤 Produciendo a: {OUTPUT_TOPIC}")
    print(f"📊 Ventana de cálculo: {WINDOW_SIZE} ticks de 1 segundo.")

    data_window = []

    for message in consumer:
        try:
            # Añadimos el nuevo tick OHLCV a la ventana
            data_window.append(message.value)

            # Mantenemos el tamaño de la ventana
            if len(data_window) > WINDOW_SIZE:
                data_window.pop(0)

            # Calculamos la señal con los datos de la ventana actual
            signal = calculate_scalping_signals(data_window)

            if signal:
                producer.send(OUTPUT_TOPIC,
                                key=f"{signal['symbol']}_{int(time.time())}".encode('utf-8'),
                                value=signal)
                producer.flush()
                print(f"✅ [Algoritmo A] Señal generada: {signal['symbol']} - {signal['signal']} @ ${signal['price']}")
            # else:
            #     print(f"📊 [Algoritmo A] Datos procesados (ventana: {len(data_window)}/{WINDOW_SIZE}) - Sin señal")

        except Exception as e:
            print(f"❌ [Algoritmo A] Error procesando mensaje: {e}")

if __name__ == "__main__":
    print(f"🚀 Iniciando Algoritmo A (Scalping)")
    print(f"📍 Bootstrap servers: {KAFKA_BOOTSTRAP_SERVERS}")
    
    consumer = create_kafka_consumer()
    producer = create_kafka_producer()
    
    process_messages(consumer, producer)