from etl.flows import retail_flow
from etl.config import DB_PATH, OUTPUT_DIR
from pathlib import Path
from loguru import logger
import os

def setup():
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    Path("logs").mkdir(exist_ok=True)
    
    if os.path.exists(DB_PATH):
        logger.info(f"База данных найдена по адресу: {DB_PATH}")
        
if __name__ == "__main__":
    setup()
    logger.info("Запуск пайплайна...")
    retail_flow()
    logger.success("Процесс завершен успешно!")