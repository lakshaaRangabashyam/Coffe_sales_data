import asyncio
from urllib.parse import quote_plus
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from dateutil.parser import parse
import csv

from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession, async_sessionmaker
from sqlalchemy.orm  import declarative_base
from sqlalchemy import Column,Date, Integer, String,Float,VARCHAR, select, update as sql_update
from sqlalchemy.future import select

DB_USER="root"
DB_PASSWORD=quote_plus("Root@123")
DB_HOST="localhost"
DB_PORT=3306
DB_NAME="coffee"

DATABASE_URL = f"mysql+aiomysql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"

 
engine = create_async_engine(DATABASE_URL, echo=True)
async_session = async_sessionmaker(engine, expire_on_commit=False,class_=AsyncSession)
Session=sessionmaker(bind=engine)
Base = declarative_base()

class Customer(Base):
    __tablename__='sales'
    transaction_date=Column(Date)
    transaction_id=Column(VARCHAR(50),primary_key=True,index=True)
    item=Column(String(100),nullable=False)
    quantity=Column(Integer,index=True)
    price_per_unit=Column(Float)
    total_spent=Column(Float)
    payment_method=Column(String(100),nullable=False)
    location=Column(String(100),nullable=100)
async def fetch():
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
        result=await conn.execute(select(Customer))
        users=result.scalars().all()
        for user in users:
            print('Created...',user.transaction_date,user.transaction_id,user.item,user.quantity,user.price_per_unit,user.total_spent,user.payment_method,user.location)

def parse_no(dt):
    try:
        return parse(dt) 
    except:
        return None
def listing(row):
    return Customer(
        transaction_date=row['transaction_date'],
        transaction_id=row['transaction_id'],
        item=row['item'],
        quantity=row['quantity'],
        price_per_unit=row['price_per_unit'],
        total_spent=row['total_spent'],
        payment_method=row['payment_method'],
        location=row['location'])
async def main():
     async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
        async with async_session() as session:
             with open('cleaned_data.csv',encoding='utf8',newline='') as csv_file:
                csvreader = csv.DictReader(csv_file, quotechar='"')
                listings = [listing(row) for row in csvreader]
                session.add_all(listings)
                await session.commit()

if __name__=='__main__':
        asyncio.run(main())