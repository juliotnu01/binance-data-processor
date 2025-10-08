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
        self.message_handlers[stream_type] = handler

    def start_all_streams(self, symbols: List[str]) -> None:
        if not symbols:
            logger.error("❌ No hay símbolos para iniciar streams")
            return

        logger.info(f"🎯 Iniciando streams WebSocket para {len(symbols)} símbolos...")

        # Binance recomienda máximo 1024 streams por conexión
        BATCH_SIZE = 1024

        for i in range(0, len(symbols), BATCH_SIZE):
            batch = symbols[i:i + BATCH_SIZE]
            logger.info(f"🔍 Procesando batch {i//BATCH_SIZE + 1}: {len(batch)} símbolos")

            # Construye la cadena para streams combinados con el separador `/`
            stream_parts = [f"{symbol.lower()}@kline_1h" for symbol in batch]
            stream_name = "/".join(stream_parts)
            logger.info(f"🔍 Streams en este batch: {stream_name[:100]}... (total {len(stream_parts)})")

            self.start_websocket(stream_name)

        logger.info(f"🎯 Streams WebSocket iniciados para todos los batches")

    def start_websocket(self, stream_name: str) -> None:
        # Construye la URL siguiendo el estándar de Binance para streams combinados
        ws_url = f"{self.config.binance.ws_url}/stream?streams={stream_name}"

        logger.info(f"🔗 URL WebSocket completa: {ws_url}")
        if len(ws_url) > 4000:
            logger.warning(f"⚠️ URL muy larga ({len(ws_url)} caracteres): podrías tener problemas de límite")

        def on_message(ws, message):
            logger.debug(f"🔔 Mensaje RAW recibido de WebSocket: {message}")
            try:
                data = json.loads(message)
                # Mensaje tipo combined stream de Binance
                if 'stream' in data and 'data' in data:
                    full_stream = data.get('stream', '')
                    stream_type = full_stream.split('@')[-1] if '@' in full_stream else ''
                    if stream_type in self.message_handlers:
                        self.message_handlers[stream_type](data)
                    else:
                        logger.warning(f"⚠️ No hay manejador para el stream type: '{stream_type}' (stream: '{full_stream}')")
                else:
                    logger.debug(f"🔔 Mensaje de control o estructura diferente: {data}")
            except Exception as e:
                logger.error(f"❌ Error procesando mensaje WebSocket: {e}")

        def on_error(ws, error):
            logger.error(f"❌ WebSocket error para stream: {error}")

        def on_close(ws, close_status_code, close_msg):
            logger.warning(f"🔌 WebSocket cerrado")
            if self.running:
                logger.info(f"🔄 Reconectando en 5 segundos...")
                time.sleep(5)
                self.start_websocket(stream_name)

        def on_open(ws):
            logger.info(f"✅ WebSocket conectado exitosamente")

        ws = websocket.WebSocketApp(
            ws_url,
            on_open=on_open,
            on_message=on_message,
            on_error=on_error,
            on_close=on_close
        )

        thread = threading.Thread(
            target=ws.run_forever,
            name=f"WebSocket-{hash(stream_name) % 10000}",
            daemon=True
        )
        thread.start()

        self.websockets[stream_name] = ws

    def stop(self) -> None:
        self.running = False
        for ws in self.websockets.values():
            try:
                ws.close()
            except:
                pass
