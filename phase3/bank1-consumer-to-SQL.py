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
    connect.execute(f'CREATE TABLE IF NOT EXISTS bank1_transactions'
                    f' (id INTEGER PRIMARY KEY AUTO_INCREMENT, '
                    f'custid INTEGER, '
                    f'bankid INTEGER, '
                    f'type VARCHAR(250) NOT NULL, '
                    f'date INTEGER, '
                    f'amt INTEGER)')
engine = create_engine(f'mysql+pymysql://{user}:{password}@localhost/{database}')
Base = declarative_base(bind=engine)

class XactionConsumer:
    def __init__(self):
        self.consumer = KafkaConsumer(
            bootstrap_servers=['localhost:9092'],
            # auto_offset_reset='earliest',
            value_deserializer=lambda m: loads(m.decode('ascii')))
        self.consumer.assign([TopicPartition('bank-customer-events', 0)])
        # These are two python dictionary's
        # Ledger is the one where all the transaction get posted
        self.ledger = {}
        # custBalances is the one where the current balance of each customer
        # account is kept.
        self.custBalances = {}


    def handleMessages(self):
        for message in self.consumer:
            message = message.value
            print('{} received'.format(message))
            self.ledger[message['custid']] = message
            # add message to the transaction table in your SQL usinf SQLalchemy
            with engine.connect() as connection:
                connection.execute(
                    f'insert into bank1_transactions ({message["custid"]}, {message["bankid"]}, {message["type"]}, {message["date"]}, {message["amt"]})')
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
