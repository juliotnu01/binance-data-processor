import time
import logging
from typing import List, Callable, Any
from concurrent.futures import ThreadPoolExecutor, as_completed

logger = logging.getLogger(__name__)

class BatchProcessor:
    def process_batches(self, items: List[Any], processor: Callable, batch_size: int = 10, delay: int = 1) -> List[Any]:
        """Procesa elementos en lotes"""
        results = []
        
        for i in range(0, len(items), batch_size):
            batch = items[i:i + batch_size]
            logger.info(f"📦 Procesando lote {i//batch_size + 1}/{(len(items)-1)//batch_size + 1}")
            
            with ThreadPoolExecutor(max_workers=batch_size) as executor:
                future_to_item = {
                    executor.submit(processor, item): item 
                    for item in batch
                }
                
                for future in as_completed(future_to_item):
                    try:
                        result = future.result()
                        results.append(result)
                    except Exception as e:
                        logger.error(f"❌ Error procesando elemento: {e}")
                        results.append(None)
            
            # Esperar entre lotes
            if i + batch_size < len(items):
                logger.info(f"⏳ Esperando {delay} segundos...")
                time.sleep(delay)
        
        return results