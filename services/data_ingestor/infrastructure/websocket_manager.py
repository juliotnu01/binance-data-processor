import json
import logging
import threading
import time
from typing import List, Dict, Any, Callable
import websocket

from config.settings import Config
from domain.entities import MarketData

logger = logging.getLogger(__name__)

class WebSocketManager:
    def __init__(self, config: Config, market_data: MarketData):
        self.config = config
        self.market_data = market_data
        self.websockets = {}
        self.running = True
        self.message_handlers: Dict[str, Callable] = {}
    
    def register_handler(self, stream_type: str, handler: Callable) -> None:
        """Registra un manejador para un tipo de stream"""
        self.message_handlers[stream_type] = handler
    
    def start_all_streams(self, symbols: List[str]) -> None:
        """Inicia streams para todos los símbolos de trading"""
        if not symbols:
            logger.error("❌ No hay símbolos para iniciar streams")
            return
            
        logger.info(f"🎯 Iniciando streams WebSocket para {len(symbols)} símbolos...")
        
        # Agrupar símbolos en streams combinados (máximo 200 por stream)
        streams = []
        for i in range(0, len(symbols), 200):
            batch = symbols[i:i + 200]
            # Suscribirse a klines de 1 hora
            stream_name = "/".join([f"{symbol.lower()}@kline_1h" for symbol in batch])
            streams.append(stream_name)
        
        # Iniciar streams
        for stream in streams:
            self.start_websocket(stream)
        
        logger.info(f"🎯 {len(streams)} streams WebSocket iniciados")
    
    def start_websocket(self, stream_name: str) -> None:
        """Inicia una conexión WebSocket para un conjunto de streams"""
        ws_url = f"{self.config.binance.ws_url}{stream_name}"
        
        def on_message(ws, message):
            try:
                data = json.loads(message)
                
                # Ignorar mensajes de control (como confirmaciones de suscripción)
                # Solo procesar mensajes que contienen datos de un stream
                if 'data' not in data:
                    logger.debug(f"🔔 Mensaje de control de WebSocket recibido: {data}")
                    return

                stream_name = data.get('stream', '')
                stream_type = stream_name.split('@')[-1] if '@' in stream_name else ''
                
                if stream_type in self.message_handlers:
                    self.message_handlers[stream_type](data)
                else:
                    # Este warning ahora es más útil, ya que solo aparecerá para streams no registrados
                    logger.warning(f"⚠️ No hay manejador para el stream type: '{stream_type}' (stream: '{stream_name}')")
            except Exception as e:
                logger.error(f"❌ Error procesando mensaje WebSocket: {e}")
                
        
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
    
    def stop(self) -> None:
        """Detiene todos los WebSockets"""
        self.running = False
        for ws in self.websockets.values():
            try:
                ws.close()
            except:
                pass