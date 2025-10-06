import os
import json
import threading
import time
import logging
from kafka import KafkaConsumer
from kafka.errors import KafkaError, NoBrokersAvailable
from flask import Flask, jsonify
from flask_socketio import SocketIO

# Configuración de logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Configuración desde variables de entorno
KAFKA_BOOTSTRAP_SERVERS = os.environ.get('KAFKA_BOOTSTRAP_SERVERS', 'kafka:29092')
INPUT_TOPICS = os.environ.get('INPUT_TOPICS', 'signals_scalping,signals_momentum,binance_user_orders').split(',')

# Inicializar aplicación Flask y SocketIO
app = Flask(__name__)
app.config['SECRET_KEY'] = 'binance_data_processor_secret'
socketio = SocketIO(app, cors_allowed_origins="*", async_mode='threading')

# Almacenamiento de datos para enviar a clientes
latest_signals = {
    'scalping': None,
    'momentum': None,
    'user_orders': None
}

# Lock para acceso thread-safe a latest_signals
signals_lock = threading.Lock()

class KafkaConsumerManager:
    """Gestiona la conexión y consumo de mensajes de Kafka"""
    
    def __init__(self, topic, bootstrap_servers):
        self.topic = topic
        self.bootstrap_servers = bootstrap_servers
        self.consumer = None
        self.running = False
        
    def create_consumer(self):
        """Crea un nuevo consumidor de Kafka con reintentos"""
        max_retries = 10
        retry_count = 0
        
        while retry_count < max_retries:
            try:
                logger.info(f"Intentando conectar consumidor para tópico: {self.topic} (intento {retry_count + 1}/{max_retries})")
                
                consumer = KafkaConsumer(
                    self.topic,
                    bootstrap_servers=self.bootstrap_servers,
                    value_deserializer=lambda m: json.loads(m.decode('utf-8')),
                    auto_offset_reset='latest',
                    enable_auto_commit=True,
                    auto_commit_interval_ms=5000,
                    group_id=f'api_gateway_{self.topic}',
                    session_timeout_ms=30000,
                    heartbeat_interval_ms=10000
                )
                
                logger.info(f"✅ Consumidor conectado exitosamente para tópico: {self.topic}")
                return consumer
                
            except NoBrokersAvailable:
                retry_count += 1
                logger.warning(f"⚠️ No hay brokers disponibles para {self.topic}. Reintento {retry_count}/{max_retries}")
                if retry_count < max_retries:
                    time.sleep(5 * retry_count)  # Backoff exponencial
                    
            except Exception as e:
                retry_count += 1
                logger.error(f"❌ Error conectando consumidor para {self.topic}: {e}")
                if retry_count < max_retries:
                    time.sleep(5 * retry_count)  # Backoff exponencial
        
        raise Exception(f"No se pudo conectar al consumidor de Kafka para {self.topic} después de {max_retries} intentos")
    
    def process_message(self, message):
        """Procesa un mensaje individual de Kafka"""
        try:
            logger.info(f"📨 Mensaje recibido de {self.topic}: {message.value.get('symbol', 'Unknown')}")
            
            # Actualizar los datos más recientes de forma thread-safe
            with signals_lock:
                if self.topic == 'signals_scalping':
                    latest_signals['scalping'] = message.value
                elif self.topic == 'signals_momentum':
                    latest_signals['momentum'] = message.value
                elif self.topic == 'binance_user_orders':
                    latest_signals['user_orders'] = message.value
            
            # Enviar a todos los clientes conectados via WebSocket
            socketio.emit('data_update', {
                'topic': self.topic,
                'data': message.value,
                'timestamp': time.time()
            })
            
            logger.info(f"✅ Mensaje procesado y enviado: {self.topic} - {message.value.get('symbol', 'Unknown')}")
            
        except Exception as e:
            logger.error(f"❌ Error procesando mensaje de {self.topic}: {e}")
    
    def start_consuming(self):
        """Inicia el consumo de mensajes del tópico"""
        self.running = True
        
        while self.running:
            try:
                # Crear o recrear el consumidor
                if not self.consumer:
                    self.consumer = self.create_consumer()
                
                logger.info(f"🎯 Iniciando consumo del tópico: {self.topic}")
                
                # Consumir mensajes
                for message in self.consumer:
                    if not self.running:
                        break
                    self.process_message(message)
                    
            except Exception as e:
                logger.error(f"🚨 Error en el consumidor de {self.topic}: {e}")
                
                # Cerrar consumidor si existe
                if self.consumer:
                    try:
                        self.consumer.close()
                    except:
                        pass
                    self.consumer = None
                
                if self.running:
                    logger.info(f"🔄 Reconectando consumidor de {self.topic} en 10 segundos...")
                    time.sleep(10)
    
    def stop_consuming(self):
        """Detiene el consumo de mensajes"""
        self.running = False
        if self.consumer:
            try:
                self.consumer.close()
            except:
                pass

    def start_kafka_consumer(topic):
        """Inicia un consumidor de Kafka en un hilo separado"""
        logger.info(f"🚀 Iniciando consumidor para tópico: {topic}")
        
        consumer_manager = KafkaConsumerManager(topic, KAFKA_BOOTSTRAP_SERVERS)
        consumer_thread = threading.Thread(
            target=consumer_manager.start_consuming,
            name=f"KafkaConsumer-{topic}"
        )
        consumer_thread.daemon = True
        consumer_thread.start()
        
        return consumer_manager

# Routes de Flask
@app.route('/')
def index():
    """Página principal"""
    return """
    <html>
        <head>
            <title>Binance Data Processor API Gateway</title>
            <style>
                body { font-family: Arial, sans-serif; margin: 40px; }
                .endpoint { background: #f5f5f5; padding: 10px; margin: 10px 0; }
            </style>
        </head>
        <body>
            <h1>📊 Binance Data Processor API Gateway</h1>
            <p>Endpoints disponibles:</p>
            <div class="endpoint">
                <strong>GET /api/signals</strong> - Señales más recientes
            </div>
            <div class="endpoint">
                <strong>GET /api/health</strong> - Estado del servicio
            </div>
            <div class="endpoint">
                <strong>GET /api/debug</strong> - Información de debugging
            </div>
            <div class="endpoint">
                <strong>WebSocket /</strong> - Actualizaciones en tiempo real
            </div>
        </body>
    </html>
    """

@app.route('/api/signals')
def get_signals():
    """Endpoint para obtener las señales más recientes"""
    with signals_lock:
        return jsonify(latest_signals)

@app.route('/api/health')
def health():
    """Endpoint de salud"""
    status = {
        "status": "healthy",
        "timestamp": time.time(),
        "kafka_bootstrap_servers": KAFKA_BOOTSTRAP_SERVERS,
        "topics": INPUT_TOPICS,
        "connected_clients": len(socketio.server.manager.rooms.get('/', {}))
    }
    return jsonify(status)

@app.route('/api/debug')
def debug():
    """Endpoint de debugging"""
    debug_info = {
        "latest_signals": latest_signals,
        "topics": INPUT_TOPICS,
        "kafka_connection": KAFKA_BOOTSTRAP_SERVERS,
        "timestamp": time.time()
    }
    
    # Verificar conexión a Kafka
    try:
        test_consumer = KafkaConsumer(
            bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
            consumer_timeout_ms=2000
        )
        topics = test_consumer.topics()
        test_consumer.close()
        
        debug_info["kafka_status"] = "connected"
        debug_info["available_topics"] = list(topics)
        
    except Exception as e:
        debug_info["kafka_status"] = "error"
        debug_info["kafka_error"] = str(e)
    
    return jsonify(debug_info)


@app.route('/monitor')
def monitor():
    """Sirve el cliente de monitoreo WebSocket actualizado"""
    return """
<!DOCTYPE html>
<html>
<head>
    <title>Binance Data Monitor</title>
    <script src="https://cdn.socket.io/4.5.0/socket.io.min.js"></script>
    <style>
        body { font-family: Arial, sans-serif; margin: 20px; background: #0f0f23; color: white; }
        .signal { padding: 15px; margin: 10px; border-radius: 8px; background: #1a1a2e; border-left: 4px solid #00aaff; }
        .trade { border-left-color: #00aaff; }
        .scalping { border-left-color: #ffaa00; }
        .momentum { border-left-color: #aa00ff; }
        #status { padding: 15px; margin: 10px 0; border-radius: 8px; text-align: center; font-weight: bold; }
        .connected { background: #155724; }
        .disconnected { background: #721c24; }
        .symbol { color: #00ffaa; font-weight: bold; font-size: 1.2em; }
        .price { color: #ffff00; font-weight: bold; font-size: 1.1em; }
    </style>
</head>
<body>
    <h1>💰 Binance Data Monitor</h1>
    <div id="status" class="disconnected">🔴 Desconectado</div>
    <div id="signals"></div>

    <script>
        const socket = io('http://localhost:8080');
        
        socket.on('connect', () => {
            document.getElementById('status').className = 'connected';
            document.getElementById('status').innerHTML = '🟢 Conectado - Recibiendo datos...';
        });
        
        socket.on('disconnect', () => {
            document.getElementById('status').className = 'disconnected';
            document.getElementById('status').innerHTML = '🔴 Desconectado';
        });
        
        socket.on('data_update', (data) => {
            const signalDiv = document.createElement('div');
            const topicClass = data.topic.includes('trade') ? 'trade' : 
                              data.topic.includes('scalping') ? 'scalping' : 
                              data.topic.includes('momentum') ? 'momentum' : 'signal';
            
            signalDiv.className = `signal ${topicClass}`;
            
            if (data.topic.includes('trade')) {
                // Datos de trade de Binance
                signalDiv.innerHTML = `
                    <div class="symbol">${data.data.s}</div>
                    <div class="price">$${data.data.p}</div>
                    <div>Cantidad: ${data.data.q}</div>
                    <div>Tipo: ${data.data.m ? 'MAKER' : 'TAKER'}</div>
                    <div>Time: ${new Date(data.data.E).toLocaleTimeString()}</div>
                    <small>Topic: ${data.topic}</small>
                `;
            } else if (data.topic.includes('signal')) {
                // Señales de trading
                signalDiv.innerHTML = `
                    <div class="symbol">${data.data.symbol}</div>
                    <div class="price">Señal: ${data.data.signal}</div>
                    <div>Precio: $${data.data.price}</div>
                    <div>Algoritmo: ${data.data.algorithm}</div>
                    <small>Topic: ${data.topic}</small>
                `;
            } else {
                // Datos genéricos
                signalDiv.innerHTML = `
                    <strong>${data.topic}</strong><br>
                    <pre>${JSON.stringify(data.data, null, 2)}</pre>
                `;
            }
            
            document.getElementById('signals').prepend(signalDiv);
        });
    </script>
</body>
</html>
"""

# Eventos de SocketIO
@socketio.on('connect')
def handle_connect():
    """Maneja la conexión de un cliente WebSocket"""
    client_id = getattr(threading.current_thread(), 'name', 'unknown')
    logger.info(f'👩‍💻 Cliente conectado via WebSocket (ID: {client_id})')
    
    # Enviar los datos más recientes al cliente que se conecta
    with signals_lock:
        for signal_type, signal_data in latest_signals.items():
            if signal_data:
                topic_name = f'signals_{signal_type}' if signal_type != 'user_orders' else 'binance_user_orders'
                socketio.emit('data_update', {
                    'topic': topic_name,
                    'data': signal_data,
                    'timestamp': time.time(),
                    'type': 'historical'
                })
                logger.info(f"📤 Enviado dato histórico para '{topic_name}' al nuevo cliente")

@socketio.on('disconnect')
def handle_disconnect():
    """Maneja la desconexión de un cliente WebSocket"""
    client_id = getattr(threading.current_thread(), 'name', 'unknown')
    logger.info(f'👋 Cliente desconectado (ID: {client_id})')

@socketio.on('ping')
def handle_ping():
    """Maneja ping del cliente"""
    return 'pong'

# Manejo de errores
@app.errorhandler(404)
def not_found(error):
    return jsonify({"error": "Endpoint no encontrado"}), 404

@app.errorhandler(500)
def internal_error(error):
    return jsonify({"error": "Error interno del servidor"}), 500

def initialize_kafka_consumers():
    """Inicializa todos los consumidores de Kafka"""
    logger.info("🔄 Inicializando consumidores de Kafka...")
    
    # Esperar a que Kafka esté listo
    logger.info("⏳ Esperando 10 segundos para que Kafka esté disponible...")
    time.sleep(10)
    
    consumer_managers = []
    
    for topic in INPUT_TOPICS:
        topic = topic.strip()
        if topic:
            try:
                manager = start_kafka_consumer(topic)
                consumer_managers.append(manager)
                logger.info(f"✅ Consumidor iniciado para: {topic}")
            except Exception as e:
                logger.error(f"❌ Error iniciando consumidor para {topic}: {e}")
    
    return consumer_managers

if __name__ == "__main__":
    logger.info("🚀 Iniciando Binance Data Processor API Gateway")
    logger.info(f"📍 Servidores Kafka: {KAFKA_BOOTSTRAP_SERVERS}")
    logger.info(f"📊 Tópicos a consumir: {INPUT_TOPICS}")
    
    # Inicializar consumidores
    consumer_managers = initialize_kafka_consumers()
    
    # Información de inicio
    logger.info("🎯 API Gateway lista para recibir conexiones")
    logger.info("🌐 Web UI disponible en: http://localhost:8080")
    logger.info("📡 WebSocket disponible en: ws://localhost:8080")
    logger.info("🔍 Debug disponible en: http://localhost:8080/api/debug")
    
    # Iniciar servidor
    try:
        socketio.run(
            app, 
            host='0.0.0.0', 
            port=8080, 
            debug=False, 
            allow_unsafe_werkzeug=True,
            log_output=True
        )
    except KeyboardInterrupt:
        logger.info("🛑 Deteniendo API Gateway...")
        for manager in consumer_managers:
            manager.stop_consuming()
        logger.info("👋 API Gateway detenido correctamente")