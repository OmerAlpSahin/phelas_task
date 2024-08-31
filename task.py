import random
from time import sleep
from datetime import datetime
from sqlalchemy import create_engine, Column, String, Float, DateTime
import sqlalchemy
from sqlalchemy.orm import sessionmaker
from datetime import timezone

# Celery configuration (used for asynchronous tasks)
# SQLAlchemy setup (using SQLite database)
engine = create_engine('sqlite:///market_data.db')
Base = sqlalchemy.orm.declarative_base()

class MarketPrice(Base):
    __tablename__ = 'prices'
    timestamp = Column(DateTime, primary_key=True)
    price = Column(Float)
    currency = Column(String)
    market = Column(String)

Base.metadata.create_all(engine)
Session = sessionmaker(bind=engine)
# Sample function to generate insights (e.g., average price)
def analyze_data(data):
    if not data:
        return "No data available."    
    total_price = sum(item.price for item in data)
    average_price = total_price / len(data)
    return {'average_price': average_price, 'max': max(data, key=lambda x: x.price).price, 'min': min(data, key=lambda x: x.price).price}

# Celery task to process the stored data asynchronously
def process_data(thread_id: int):
    session = Session()
    data = session.query(MarketPrice).all()
    insights = analyze_data(data)
    print(f"Calculating insights...... {thread_id}")
    sleep(random.randint(3, 8))
    print(f"Insights: {insights} - {thread_id}")
    return insights


# Function to fetch data from the external API
def fetch_data():
    data = {"timestamp": datetime.now(tz=timezone.utc).isoformat(),"price":random.random() * 100  ,"currency":"USD", "market": "Intraday"}
    return data

# Function to store fetched data into the database
def store_data(data):
    session = Session()
    market_price = MarketPrice(
        timestamp=datetime.fromisoformat(data['timestamp'].replace('Z', '+00:00')),
        price=data['price'],
        currency=data['currency'],
        market=data['market']
    )
    session.add(market_price)
    session.commit()
    
# Scheduler to regularly fetch and store data
thread_counter = 0
def scheduled_fetch(thread_counte):
    print("Fetching data...")
    data = fetch_data()
    store_data(data)
    
    process_data(thread_counte)
    
    sleep(10)
    # Call Celery task to process data asynchronously
# Start the scheduler


if __name__ == "__main__":
    from threading import Thread
    while True:
        Thread(target=scheduled_fetch, args=(thread_counter,)).start()
        thread_counter += 1

        sleep(2)