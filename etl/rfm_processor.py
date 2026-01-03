import pandas as pd
from datetime import datetime
from etl import config 
from loguru import logger

class RFMProcessor:
    def clean_data(self, df):
        logger.info("ETL: Очистка данных Online Retail...")
        df = df.dropna(subset=['CustomerID'])
        df = df[df['Quantity'] > 0]
        df['InvoiceDate'] = pd.to_datetime(df['InvoiceDate'])
        df['Total_Price'] = df['Quantity'] * df['UnitPrice']
        return df

    def calculate(self, df):
        logger.info("Запуск RFM-анализа...")
        last_date = df['InvoiceDate'].max()
        
        rfm = df.groupby('CustomerID').agg({
            'InvoiceDate': lambda x: (last_date - x.max()).days,
            'InvoiceNo': 'nunique',
            'Total_Price': 'sum'
        }).rename(columns={
            'InvoiceDate': 'recency', 
            'InvoiceNo': 'frequency', 
            'Total_Price': 'monetary'
        })
        
        rfm['segment'] = rfm.apply(self._assign_segment, axis=1)
        return rfm.reset_index()

    def _assign_segment(self, row):
        if row['monetary'] > config.CHAMPION_THRESHOLD: return 'Champion'
        if row['recency'] > config.AT_RISK_DAYS: return 'At Risk'
        if row['monetary'] < config.LOWSPENDER_THRESHOLD: return 'Lowspender'
        return 'Regular'

    def calculate_abc_by_country(self, df):
        logger.info("ABC-анализ выручки по странам...")
        abc = df.groupby('Country')['Total_Price'].sum().reset_index()
        abc = abc.sort_values('Total_Price', ascending=False)
        abc['share'] = abc['Total_Price'] / abc['Total_Price'].sum()
        abc['cum_share'] = abc['share'].cumsum()
        abc['abc_category'] = abc['cum_share'].apply(
            lambda x: 'A' if x <= 0.8 else ('B' if x <= 0.95 else 'C')
        )
        return abc

    def detect_anomalies(self, df):
        logger.info("Поиск аномально дорогих заказов...")
        orders = df.groupby('InvoiceNo')['Total_Price'].sum().reset_index()
        mean = orders['Total_Price'].mean()
        std = orders['Total_Price'].std()
        anomalies = orders[orders['Total_Price'] > (mean + 3 * std)]
        return anomalies