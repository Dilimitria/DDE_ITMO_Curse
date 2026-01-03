import matplotlib.pyplot as plt
import seaborn as sns
from loguru import logger
from etl.config import OUTPUT_DIR

class DataAnalyzer:
    def create_plots(self, rfm_df, abc_country_df):
        logger.info("Создание графиков...")
        plt.style.use('ggplot')
        fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(20, 8))

        counts = rfm_df['segment'].value_counts()
        ax1.pie(counts, labels=counts.index, autopct='%1.1f%%', startangle=140)
        ax1.set_title('Доля сегментов покупателей')

        top_countries = abc_country_df.head(10)
        sns.barplot(data=top_countries, x='Total_Price', y='Country', 
                    hue='abc_category', ax=ax2)
        ax2.set_title('Топ-10 стран по выручке (ABC)')
        
        plt.tight_layout()
        plt.savefig(OUTPUT_DIR / 'retail_main_report.png')
        plt.close()

    def create_eda_report(self, df):
        plt.figure(figsize=(12, 6))
        sns.histplot(df[df['UnitPrice'] < 20]['UnitPrice'], kde=True, color='green')
        plt.title('Распределение цен на товары (до 20 GBP)')
        plt.savefig(OUTPUT_DIR / 'price_distribution.png')
        plt.close()