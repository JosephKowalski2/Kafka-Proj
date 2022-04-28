from kafka import KafkaConsumer, TopicPartition
from json import loads
from statistics import mean, StatisticsError, stdev


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
        self.limit = -5000



    def handleMessages(self):
        for message in self.consumer:
            message = message.value
            print('{} received'.format(message))
            self.ledger[message['custid']] = message
            if message['custid'] not in self.custBalances:
                self.custBalances[message['custid']] = 0
            if message['type'] == 'dep':
                self.custBalances[message['custid']] += message['amt']
            else:
                self.custBalances[message['custid']] -= message['amt']
            for id, value in self.custBalances.items():
                if value < self.limit:
                    print('Customer ' + str(id) + ' is over limit by', value)
            print(self.custBalances)



if __name__ == "__main__":
    c = XactionConsumer()
    c.handleMessages()
