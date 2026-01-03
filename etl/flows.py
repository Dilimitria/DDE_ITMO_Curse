from prefect import flow, task
import pandas as pd
from etl import config  
from etl.rfm_processor import RFMProcessor  
from etl.database import Database  
from etl.data_analyzer import DataAnalyzer 
from loguru import logger

@task
def load_and_clean():
    """Загрузка и базовая очистка данных"""
    df = pd.read_csv(config.RAW_DATA_PATH)
    proc = RFMProcessor()
    return proc.clean_data(df)

@task(persist_result=False)
def save_cleaned_data(df, db_engine):
    """Сохранение 'Silver' слоя — очищенные транзакции"""
    df.to_sql('cleaned_transactions', db_engine, if_exists='replace', index=False)
    logger.info("Слой 'Silver' (cleaned_transactions) успешно сохранен в базу.")

@task
def run_analytics(df):
    """Расчет аналитических витрин"""
    proc = RFMProcessor()
    rfm = proc.calculate(df)
    abc = proc.calculate_abc_by_country(df)
    anomalies = proc.detect_anomalies(df)
    return rfm, abc, anomalies

@task
def save_marts_to_db(rfm, abc, anomalies):
    """Сохранение 'Gold' слоя — витрины данных"""
    db = Database()
    db.save_dataframe(rfm, 'mart_customer_segments')
    db.save_dataframe(abc, 'mart_countries_abc')
    db.save_dataframe(anomalies, 'audit_order_anomalies')
    logger.info("Слой 'Gold' (витрины данных) успешно сохранен.")

@flow(name="Retail_ETL_Full")
def retail_flow():
    db = Database()
    df_clean = load_and_clean()
    save_cleaned_data(df_clean, db.engine)
    
    rfm, abc, anomalies = run_analytics(df_clean)
    save_marts_to_db(rfm, abc, anomalies)
    
    logger.info("ETL процесс успешно завершен!")