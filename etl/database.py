from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from etl.config import DB_PATH  
from etl.models import Base  
from sqlalchemy import Column, Integer, String, Float, DateTime
from sqlalchemy.ext.declarative import declarative_base

class Database:
    def __init__(self):
        self.engine = create_engine(f'sqlite:///{DB_PATH}')
        Base.metadata.create_all(self.engine)
        self.SessionLocal = sessionmaker(bind=self.engine)

    def save_dataframe(self, df, table_name):
        df.to_sql(table_name, self.engine, if_exists='replace', index=False)

Base = declarative_base()

class CleanedTransaction(Base):
    __tablename__ = 'cleaned_transactions'
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    InvoiceNo = Column(String)
    StockCode = Column(String)
    Description = Column(String)
    Quantity = Column(Integer)
    InvoiceDate = Column(DateTime)
    UnitPrice = Column(Float)
    CustomerID = Column(Float)
    Country = Column(String)
    Total_Price = Column(Float)