import time
import json
import random
from kafka import KafkaProducer

KAFKA_BROKER = '192.168.56.104:30092' 
TOPIC_NAME = 'test'

print(f"Đang kết nối tới Kafka tại {KAFKA_BROKER}...")

try:
    producer = KafkaProducer(
        bootstrap_servers=[KAFKA_BROKER],
        value_serializer=lambda x: json.dumps(x).encode('utf-8'),
        request_timeout_ms=5000,
        api_version_auto_timeout_ms=5000
    )
    print("Kết nối thành công!")
except Exception as e:
    print(f"Lỗi kết nối: {e}")
    exit()

def generate_data():
    return {
        'sensor_id': random.randint(1, 100),
        'temperature': round(random.uniform(20.0, 45.0), 2),
        'humidity': round(random.uniform(30.0, 90.0), 2),
        'timestamp': time.time()
    }

try:
    i = 0
    while True:
        data = generate_data()
        producer.send(TOPIC_NAME, value=data)
        
        print(f"[{i}] Đã gửi: {data}")
        i += 1
        time.sleep(2)

except KeyboardInterrupt:
    print("\nĐã dừng Producer.")
    producer.close()