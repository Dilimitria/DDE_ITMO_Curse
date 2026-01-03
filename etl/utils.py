import time
from functools import wraps
from loguru import logger

def retry_decorator(retries=3, delay=2):
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            for i in range(retries):
                try:
                    return func(*args, **kwargs)
                except Exception as e:
                    logger.warning(f"Попытка {i+1} не удалась. Рестарт...")
                    time.sleep(delay)
            raise Exception("Все попытки исчерпаны")
        return wrapper
    return decorator