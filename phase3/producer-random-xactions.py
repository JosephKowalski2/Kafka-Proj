from time import sleep
from json import dumps
from kafka import KafkaProducer
import time
import random

class Producer:
    def __init__(self):
        self.producer = KafkaProducer(bootstrap_servers=['localhost:9092'], value_serializer=lambda m: dumps(m).encode('ascii'))

    def emit1(self, cust=55, type="dep"):
        data = {'custid': random.randint(50,56),
            'bankid': 1,
            'type': self.depOrWth(),
            'date': int(time.time()),
            'amt': random.randint(10,101)*100,
            }
        return data

    def emit2(self, cust=55, type="dep"):
        data = {'custid': random.randint(50,56),
            'bankid': 2,
            'type': self.depOrWth(),
            'date': int(time.time()),
            'amt': random.randint(10,101)*100,
            }
        return data

    def depOrWth(self):
        return 'dep' if (random.randint(0,2) == 0) else 'wth'

    def generateRandomXactions1(self, n=1000):
        for _ in range(n):
            data = self.emit1()
            print('sent', data)
            self.producer.send('bank-customer-events', value=data, partition=0)
            sleep(1)

    def generateRandomXactions2(self, n=1000):
        for _ in range(n):
            data = self.emit2()
            print('sent', data)
            self.producer.send('bank-customer-events', value=data, partition=1)
            sleep(1)

if __name__ == "__main__":
    p = Producer()
    p.generateRandomXactions1(n=10)
    p.generateRandomXactions2(n=10)
