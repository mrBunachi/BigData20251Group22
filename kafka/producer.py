import json
import time
import sys
from kafka import KafkaProducer

KAFKA_BROKER = '192.168.56.104:30092' 
DATA_FILE = '..\data\job_data_1.json'

# Khởi tạo Producer
producer = KafkaProducer(
    bootstrap_servers=[KAFKA_BROKER],
    value_serializer=lambda x: json.dumps(x).encode('utf-8'),
    batch_size=16384,
    linger_ms=10 
)

def load_data():
    try:
        with open(DATA_FILE, 'r', encoding='utf-8') as f:
            data = json.load(f)
            if isinstance(data, dict): 
                return [data] 
            return data
    except FileNotFoundError:
        print(f"Không tìm thấy file '{DATA_FILE}'. Hãy kiểm tra lại đường dẫn.")
        return []

# Chế độ 1: Nạp lịch sử (Batch) -> Topic: itjobs_history
def run_batch_mode(jobs):
    TOPIC = 'itjobs_history'
    print(f"[BATCH MODE] Đang nạp {len(jobs)} jobs vào topic '{TOPIC}'...")
    
    for i, job in enumerate(jobs):
        job['ingest_time'] = str(time.time())
        
        producer.send(TOPIC, value=job)
        
        if (i + 1) % 100 == 0:
            print(f" -> Đã gửi {i + 1} jobs...")
            
    producer.flush()
    print(f"Đã nạp xong {len(jobs)} bản ghi vào lịch sử.")

# Chế độ 2: Giả lập Streaming -> Topic: itjobs_live
def run_streaming_mode(jobs):
    TOPIC = 'itjobs_live'
    print(f"[STREAM MODE] Đang giả lập dữ liệu vào topic '{TOPIC}'...")
    
    while True:
        for job in jobs:
            job['ingest_time'] = str(time.time())
            
            producer.send(TOPIC, value=job)
            
            title = job.get('title') or job.get('Tiêu đề') or job.get('Tên công việc') or 'Unknown Job'
            print(f" -> Live Job: {title}")
            
            time.sleep(2) 

if __name__ == "__main__":
    data = load_data()
    if not data:
        sys.exit(1)

    if len(sys.argv) > 1 and sys.argv[1] == 'batch':
        run_batch_mode(data)
    else:
        run_streaming_mode(data)