import os
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent.parent

RAW_DATA_PATH = BASE_DIR / "data" / "online_retail.csv"
DB_PATH = BASE_DIR / "data" / "online_retail_360.db"

OUTPUT_DIR = BASE_DIR / "eda" / "results" 

CHAMPION_THRESHOLD = 2000
LOWSPENDER_THRESHOLD = 500

AT_RISK_DAYS = 180
