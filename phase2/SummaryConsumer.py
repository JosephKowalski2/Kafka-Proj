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
        self.deposit = []
        self.withdrawal = []

    def mean_deposits(self):
        try:
            return mean(self.deposit)
        except StatisticsError:
            return 0

    def mean_withdrawals(self):
        try:
            return mean(self.withdrawal)
        except StatisticsError:
            return 0

    def dev_deposits(self):
        try:
            return stdev(self.deposit)
        except StatisticsError:
            return 0

    def dev_withdrawals(self):
        try:
            return stdev(self.withdrawal)
        except StatisticsError:
            return 0

    def handleMessages(self):
        for message in self.consumer:
            message = message.value
            print('{} received'.format(message))
            self.ledger[message['custid']] = message
            if message['custid'] not in self.custBalances:
                self.custBalances[message['custid']] = 0
            if message['type'] == 'dep':
                self.custBalances[message['custid']] += message['amt']
                self.deposit.append(message['amt'])
            else:
                self.custBalances[message['custid']] -= message['amt']
                self.withdrawal.append(message['amt'])
            # print(self.custBalances)
            # print(self.deposit)
            # print(self.withdrawal)
            print('Mean of the deposits is: ' + str(round(XactionConsumer.mean_deposits(self), 2)))
            print('Mean of the withdrawals is: ' + str(round(XactionConsumer.mean_withdrawals(self), 2)))
            print('Standard deviation of deposits is: ' + str(round(XactionConsumer.dev_deposits(self), 2)))
            print('Standard deviation of withdrawals is: ' + str(round(XactionConsumer.dev_withdrawals(self), 2)))

if __name__ == "__main__":
    c = XactionConsumer()
    c.handleMessages()
