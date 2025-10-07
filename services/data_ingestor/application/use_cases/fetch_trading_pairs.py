import logging
from typing import List

from domain.entities import TradingPair
from infrastructure.binance_client import BinanceClientInterface

logger = logging.getLogger(__name__)

class FetchTradingPairsUseCase:
    def __init__(self, binance_client: BinanceClientInterface, max_leverage: int = 75):
        self.binance_client = binance_client
        self.max_leverage = max_leverage
    
    def execute(self, max_pairs: int = 50) -> List[TradingPair]:
        logger.info("🔄 Obteniendo pares de trading USDT...")
        
        # 1. Obtener información del exchange (endpoint público)
        exchange_info = self.binance_client.get_exchange_info()
        if not exchange_info:
            raise Exception("No se pudo obtener información del exchange")
        
        symbols = exchange_info.get('symbols', [])
        
        # 2. Filtrar pares USDT PERPETUAL en trading
        usdt_pairs = [
            symbol for symbol in symbols
            if (symbol.get('quoteAsset') == 'USDT' and
                symbol.get('status') == 'TRADING' and
                symbol.get('contractType') == 'PERPETUAL')
        ]
        
        logger.info(f"📊 Encontrados {len(usdt_pairs)} pares USDT PERPETUAL")
        
        # 3. Obtener información de apalancamiento (endpoint firmado)
        leverage_brackets = self.binance_client.get_leverage_brackets()
        leverage_dict = {}
        
        if leverage_brackets:
            for bracket in leverage_brackets:
                symbol = bracket.get('symbol')
                if symbol and 'brackets' in bracket:
                    max_lev = max(b.get('initialLeverage', 0) for b in bracket['brackets'])
                    leverage_dict[symbol] = max_lev
        
        # 4. Crear objetos de dominio y filtrar por apalancamiento
        trading_pairs = []
        for symbol in usdt_pairs:
            symbol_name = symbol['symbol']
            max_lev = leverage_dict.get(symbol_name, 0)
            
            # Incluir solo si cumple con el apalancamiento mínimo
            if max_lev >= self.max_leverage or not leverage_dict:
                pair = TradingPair(
                    symbol=symbol_name,
                    base_asset=symbol.get('baseAsset', ''),
                    quote_asset=symbol.get('quoteAsset', ''),
                    filters=symbol.get('filters', []),
                    max_leverage=max_lev
                )
                trading_pairs.append(pair)
        
        # Ordenar por símbolo (podría ordenarse por otro criterio)
        trading_pairs.sort(key=lambda x: x.symbol)
        
        # Limitar número de pares
        selected_pairs = trading_pairs[:max_pairs]
        
        logger.info(f"🎯 Seleccionados {len(selected_pairs)} pares con apalancamiento >= {self.max_leverage}")
        for i, pair in enumerate(selected_pairs[:10]):
            logger.info(f"   {i+1}. {pair.symbol} - Apalancamiento máximo: {pair.max_leverage}")
        
        return selected_pairs