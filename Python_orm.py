import sqlalchemy
from sqlalchemy.orm import sessionmaker
from models import create_tables, Publisher, Book, Stock, Shop, Sale
import json



DSN = 'postgresql://postgres:postgres@localhost:5432/orm_bd'
engine = sqlalchemy.create_engine(DSN)
create_tables(engine)

Session = sessionmaker(bind=engine)
session = Session()

with open('test_data.json', 'r') as fd:
    data = json.load(fd)

for record in data:
    model = {
        'publisher': Publisher,
        'shop': Shop,
        'book': Book,
        'stock': Stock,
        'sale': Sale,
    }[record.get('model')]
    session.add(model(id=record.get('pk'), **record.get('fields')))
session.commit()

con = Publisher.id == input("Введите id издателя (от 1 до 4): ")
query = session.query(Book.title, Shop.name, Sale.price,
        Sale.date_sale).join(Publisher).join(Stock).join(Sale).join(Shop).filter(con)

for book, shop, price, date in query:
    print(f"{book} | {shop} | {price} | {date}")

session.close()