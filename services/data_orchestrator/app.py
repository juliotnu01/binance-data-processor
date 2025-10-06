import os
import requests
import time
import hmac
import hashlib

API_KEY = os.environ.get('BINANCE_API_KEY')
API_SECRET = os.environ.get('BINANCE_API_SECRET')
MAX_LEVERAGE = int(os.environ.get('MAX_LEVERAGE', '75'))
SYMBOLS_FILE_PATH = os.environ.get('SYMBOLS_FILE_PATH', '/app/data/symbols.txt')
BASE_URL = os.environ.get('BASE_URL', 'https://fapi.binance.com')

def create_signature(query_string: str) -> str:
    return hmac.new(
        API_SECRET.encode('utf-8'),
        query_string.encode('utf-8'),
        hashlib.sha256
    ).hexdigest()

def fetch_usdt_perpetual_pairs():
    print("🔍 Obteniendo pares de trading USDT de Binance Futures...")
    try:
        exchange_info_response = requests.get(f"{BASE_URL}/fapi/v1/exchangeInfo")
        exchange_info_response.raise_for_status()
        symbols = exchange_info_response.json().get('symbols', [])
        usdt_perpetuals = [s for s in symbols if s.get('quoteAsset') == 'USDT' and s.get('status') == 'TRADING' and s.get('contractType') == 'PERPETUAL']
        print(f"📊 Encontrados {len(usdt_perpetuals)} pares USDT perpetuos en trading.")

        print("🔐 Obteniendo información de apalancamiento (requiere API Key)...")
        if not API_KEY or not API_SECRET:
            print("⚠️ Claves API no encontradas. Omitiendo filtro de apalancamiento.")
            return [s['symbol'] for s in usdt_perpetuals]

        timestamp = int(time.time() * 1000)
        query_string = f"timestamp={timestamp}"
        signature = create_signature(query_string)
        headers = {'X-MBX-APIKEY': API_KEY}
        leverage_url = f"{BASE_URL}/fapi/v1/leverageBracket?{query_string}&signature={signature}"
        leverage_response = requests.get(leverage_url, headers=headers)
        leverage_response.raise_for_status()
        leverage_brackets = leverage_response.json()

        valid_symbols = []
        for symbol_info in usdt_perpetuals:
            symbol = symbol_info['symbol']
            bracket = next((b for b in leverage_brackets if b['symbol'] == symbol), None)
            if bracket:
                max_lev = max(b['initialLeverage'] for b in bracket['brackets'])
                if max_lev >= MAX_LEVERAGE:
                    valid_symbols.append(symbol)
            else:
                print(f"⚠️ No se encontró info de apalancamiento para {symbol}, excluyendo.")
                
        print(f"✅ Seleccionados {len(valid_symbols)} pares con apalancamiento >= {MAX_LEVERAGE}.")
        return valid_symbols

    except requests.exceptions.RequestException as e:
        print(f"❌ Error al contactar la API de Binance: {e}")
        print("   Haciendo fallback a todos los símbolos USDT perpetuos encontrados.")
        return [s['symbol'] for s in usdt_perpetuals]
    except Exception as e:
        print(f"❌ Error inesperado: {e}")
        return []

def save_symbols_to_file(symbols):
    print(f"💾 Guardando {len(symbols)} símbolos en {SYMBOLS_FILE_PATH}")
    os.makedirs(os.path.dirname(SYMBOLS_FILE_PATH), exist_ok=True)
    with open(SYMBOLS_FILE_PATH, 'w') as f:
        for symbol in symbols:
            f.write(f"{symbol}\n")
    print("✅ Símbolos guardados exitosamente.")

if __name__ == "__main__":
    symbols = fetch_usdt_perpetual_pairs()
    if symbols:
        save_symbols_to_file(symbols)
    else:
        print("🚨 No se pudieron obtener los símbolos. El archivo no será creado.")