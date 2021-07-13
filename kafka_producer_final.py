import pandas as pd
import time
from kafka import KafkaProducer

# Set Kafka config
kafka_broker_hostname='localhost'
kafka_broker_portno='9092'
kafka_broker=kafka_broker_hostname + ':' + kafka_broker_portno
kafka_topic='air_quality'

data_send_interval=5


if __name__ == "__main__":
    # Create KafkaProducer instance
    producer = KafkaProducer(bootstrap_servers=kafka_broker)

    # Load demo data
    # air_1 = pd.read_csv('air1.csv')
    air_2 = pd.read_csv('/opt/city_hour_final_new.csv')

    # Send demo data to Kafka broker
    for _index in range(0, len(air_2)):
        # json_air_1 = air_1[air_1.index==_index].to_json(orient='records')[1:-1]
        # producer.send(kafka_topic, bytes(json_air_1, 'utf-8'))
        # print(json_air_1)

        json_air_2 = air_2[air_2.index==_index].to_json(orient='records')[1:-1]
        producer.send(kafka_topic, bytes(json_air_2, 'utf-8'))
        print(json_air_2)
        # json_iot_id12 = iot_data_id12[iot_data_id12.index==_index].to_json(orient='records')[1:-1]
        # producer.send(kafka_topic, bytes(json_iot_id12, 'utf-8'))
        # print(json_iot_id12)
        time.sleep(data_send_interval)