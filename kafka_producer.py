import json
import random
import time
from kafka import KafkaProducer

def generate_sensor_data():
    return {
        "sensor_id": random.randint(1, 10),
        "temperature": round(random.uniform(20, 30), 2),
        "humidity": round(random.uniform(30, 70), 2),
        "timestamp": int(time.time())
    }

producer = KafkaProducer(
    bootstrap_servers=["localhost:9092"],
    value_serializer=lambda value: json.dumps(value).encode("utf-8")
)
try:
    while True:
        data = generate_sensor_data()
        producer.send("sensor_data", value=data)
        producer.flush()
        print(f"Sent: {data}", flush=True)
        time.sleep(1)
except KeyboardInterrupt:
    print("\nProductor detenido.")
finally:
    producer.close()
