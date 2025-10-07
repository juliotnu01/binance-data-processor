import requests
import time
import hmac
import hashlib
import logging
from typing import List, Dict, Any, Optional
from abc import ABC, abstractmethod

from config.settings import Config

logger = logging.getLogger(__name__)

class BinanceClientInterface(ABC):
    @abstractmethod
    def get_exchange_info(self) -> Optional[Dict[str, Any]]:
        pass
    
    @abstractmethod
    def get_leverage_brackets(self) -> Optional[List[Dict[str, Any]]]:
        pass
    
    @abstractmethod
    def get_klines(self, symbol: str, interval: str, limit: int) -> Optional[List[List[Any]]]:
        pass

class BinanceClient(BinanceClientInterface):
    def __init__(self, config: Config):
        self.config = config
        self.api_base = config.binance.api_base
        self.api_key = config.binance.api_key
        self.api_secret = config.binance.api_secret
        self.timeout = 10
    
    def _create_signature(self, query_string: str) -> str:
        """Crea la firma HMAC SHA256 para una petición a la API de Binance"""
        return hmac.new(
            self.api_secret.encode('utf-8'),
            query_string.encode('utf-8'),
            hashlib.sha256
        ).hexdigest()
    
    def _make_request(self, endpoint: str, params: Dict[str, Any] = None, signed: bool = False) -> Optional[Dict[str, Any]]:
        try:
            url = f"{self.api_base}/{endpoint}"
            headers = {}
            
            if signed:
                if not self.api_key or not self.api_secret:
                    logger.warning("⚠️ BINANCE_API_KEY o BINANCE_API_SECRET no encontrados. No se puede realizar la petición firmada.")
                    return None
                
                timestamp = int(time.time() * 1000)
                if params is None:
                    params = {}
                params['timestamp'] = timestamp
                
                query_string = '&'.join([f"{k}={v}" for k, v in params.items()])
                signature = self._create_signature(query_string)
                
                headers['X-MBX-APIKEY'] = self.api_key
                url = f"{url}?{query_string}&signature={signature}"
            elif params:
                query_string = '&'.join([f"{k}={v}" for k, v in params.items()])
                url = f"{url}?{query_string}"
            
            response = requests.get(url, headers=headers, timeout=self.timeout)
            response.raise_for_status()
            return response.json()
        except Exception as e:
            logger.error(f"❌ Error en request a {endpoint}: {e}")
            return None
    
    def get_exchange_info(self) -> Optional[Dict[str, Any]]:
        return self._make_request("fapi/v1/exchangeInfo")
    
    def get_leverage_brackets(self) -> Optional[List[Dict[str, Any]]]:
        return self._make_request("fapi/v1/leverageBracket", signed=True)
    
    def get_klines(self, symbol: str, interval: str, limit: int) -> Optional[List[List[Any]]]:
        params = {
            "symbol": symbol,
            "interval": interval,
            "limit": limit
        }
        return self._make_request("fapi/v1/klines", params)