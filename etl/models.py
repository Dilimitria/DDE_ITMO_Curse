from sqlalchemy import Column, Integer, Float, String, DateTime
from sqlalchemy.ext.declarative import declarative_base

Base = declarative_base()

class CustomerSegment(Base):
    __tablename__ = 'mart_customer_segments'
    
    customer_id = Column(Integer, primary_key=True)
    recency = Column(Integer)
    frequency = Column(Integer)
    monetary = Column(Float)
    segment = Column(String)