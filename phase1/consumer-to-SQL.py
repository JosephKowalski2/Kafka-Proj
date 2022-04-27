import getpass
from kafka import KafkaConsumer, TopicPartition
from json import loads
from sqlalchemy import create_engine
from sqlalchemy.ext.declarative import declarative_base

database = 'zipbank'
user = getpass.getuser()
password = getpass.getpass(stream=None)
db_engine = create_engine(f'mysql+pymysql://{user}:{password}@localhost')
with db_engine.connect() as connect:
    connect.execute(f'CREATE DATABASE IF NOT EXISTS {database}')
    connect.execute(f'USE {database}')
    connect.execute(f'CREATE TABLE IF NOT EXISTS transactions'
                    f' (id INTEGER PRIMARY KEY AUTO_INCREMENT, '
                    f'custid INTEGER, type VARCHAR(250) NOT NULL, '
                    f'date INTEGER, '
                    f'amt INTEGER)')
engine = create_engine(f'mysql+pymysql://{user}:{password}@localhost/{database}')
Base = declarative_base(bind=engine)

class XactionConsumer:
    def __init__(self):
        self.consumer = KafkaConsumer('bank-customer-events',
            bootstrap_servers=['localhost:9092'],
            # auto_offset_reset='earliest',
            value_deserializer=lambda m: loads(m.decode('ascii')))
        # These are two python dictionary's
        # Ledger is the one where all the transaction get posted
        self.ledger = {}
        # custBalances is the one where the current balance of each customer
        # account is kept.
        self.custBalances = {}
        # THE PROBLEM is every time we re-run the Consumer, ALL our customer
        # data gets lost!
        # add a way to connect to your database here.

        #Go back to the readme.

    def handleMessages(self):
        for message in self.consumer:
            message = message.value
            print('{} received'.format(message))
            self.ledger[message['custid']] = message
            # add message to the transaction table in your SQL usinf SQLalchemy
            with engine.connect() as connection:
                connection.execute('insert into transactions ({0}, {1}, {2}, {3})'.format(message['custid'], message['type'], message['date'], message['amt']))
            if message['custid'] not in self.custBalances:
                self.custBalances[message['custid']] = 0
            if message['type'] == 'dep':
                self.custBalances[message['custid']] += message['amt']
            else:
                self.custBalances[message['custid']] -= message['amt']
            print(self.custBalances)



if __name__ == "__main__":
    Base.metadata.create_all(engine)
    c = XactionConsumer()
    c.handleMessages()
