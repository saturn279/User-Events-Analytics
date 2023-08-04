from argparse import ArgumentParser, FileType
from configparser import ConfigParser
from confluent_kafka import Producer
import pandas as pd
import time

if __name__ == '__main__':
    parser = ArgumentParser()
    parser.add_argument('config_file', type=FileType('r'))
    args = parser.parse_args()

    config_parser = ConfigParser()
    config_parser.read_file(args.config_file)
    config = dict(config_parser['default'])

    producer = Producer(config)


    def delivery_callback(err, msg):
        if err:
            print('ERROR: Message failed delivery: {}'.format(err))
        else:
            print("Produced event to topic {topic}: key = {key:12} value = {value:12}".format(
                topic=msg.topic(), key=msg.key().decode('utf-8'), value=msg.value().decode('utf-8')))


    topic = "user_activity"
    df = pd.read_csv('2019-Oct_sample.csv')

    for i in range(len(df)):
        producer.produce(topic, df.iloc[i].to_json(), "sim_producer", callback=delivery_callback)
        time.sleep(2)
    producer.poll(10000)
    producer.flush()
