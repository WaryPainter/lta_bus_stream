import time
import json
import requests
import os
from dotenv import load_dotenv
from confluent_kafka import Producer
from confluent_kafka.admin import AdminClient, NewTopic

# Load configuration from .env
load_dotenv()
LTA_ACCOUNT_KEY = os.getenv('LTA_ACCOUNT_KEY')
KAFKA_BOOTSTRAP_SERVERS = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092')
TOPIC_NAME = 'lta-bus-arrivals'

# Sample Bus Stops to monitor (prototype)
BUS_STOPS = ['80231', '80239', '81091', '81099']

def create_topic():
    """Create the Kafka topic if it doesn't already exist."""
    admin_client = AdminClient({'bootstrap.servers': KAFKA_BOOTSTRAP_SERVERS})
    new_topic = NewTopic(TOPIC_NAME, num_partitions=1, replication_factor=1)
    fs = admin_client.create_topics([new_topic])
    for topic, f in fs.items():
        try:
            f.result()
            print(f"Topic '{topic}' created successfully.")
        except Exception as e:
            if "already exists" in str(e):
                print(f"Topic '{topic}' already exists.")
            else:
                print(f"Failed to create topic: {e}")

def delivery_report(err, msg):
    """Callback for Kafka message delivery confirmation."""
    if err is not None:
        print(f"Message delivery failed: {err}")
    else:
        print(f"Message delivered to {msg.topic()} [{msg.partition()}] at offset {msg.offset()}")

def fetch_and_produce():
    """Fetch data from LTA API and push to Kafka."""
    producer = Producer({'bootstrap.servers': KAFKA_BOOTSTRAP_SERVERS})
    headers = {'AccountKey': LTA_ACCOUNT_KEY, 'accept': 'application/json'}

    print(f"Starting producer... Monitoring {len(BUS_STOPS)} stops.")
    try:
        while True:
            for stop in BUS_STOPS:
                url = f"https://datamall2.mytransport.sg/ltaodataservice/v3/BusArrival?BusStopCode={stop}"
                try:
                    res = requests.get(url, headers=headers, timeout=10)
                    if res.status_code == 200:
                        data = res.json()
                        # Produce the entire JSON payload to Kafka
                        producer.produce(
                            TOPIC_NAME, 
                            key=stop, 
                            value=json.dumps(data), 
                            callback=delivery_report
                        )
                    else:
                        print(f"Error fetching stop {stop}: {res.status_code}")
                except Exception as e:
                    print(f"Request failed for stop {stop}: {e}")
                
            producer.flush()
            print("Batch sent. Waiting 60 seconds...")
            time.sleep(60)
    except KeyboardInterrupt:
        print("Producer stopped by user.")

if __name__ == "__main__":
    if not LTA_ACCOUNT_KEY or LTA_ACCOUNT_KEY == 'YOUR_ACTUAL_API_KEY_HERE':
        print("Error: Please set your LTA_ACCOUNT_KEY in the .env file.")
    else:
        create_topic()
        fetch_and_produce()
