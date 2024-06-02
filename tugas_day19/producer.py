from kafka import KafkaProducer
import json
import time
import random

producer = KafkaProducer(bootstrap_servers='localhost:9092',
                         value_serializer=lambda v: json.dumps(v).encode('utf-8'))

def produce_purchase_events():
    while True:
        event = {
            'timestamp': int(time.time() * 1000),
            'amount': random.uniform(10.0, 100.0)
        }
        producer.send('purchases', event)
        print(f"Produced: {event}")
        time.sleep(1)

if __name__ == "__main__":
    produce_purchase_events()
