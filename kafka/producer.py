import json
import time
<<<<<<< HEAD
import os
=======
import sys
import random
>>>>>>> 2e65b52ad13febf25a0dacbff2843b1d53fedde8
from kafka import KafkaProducer

# 1. Cấu hình Kafka
KAFKA_BROKER = '192.168.56.104:30092' 
DATA_FILE = 'data\job_data_1.json'

<<<<<<< HEAD
# 2. Xác định đường dẫn thư mục chứa data
# Lấy đường dẫn của file code hiện tại (trong folder kafka)
current_dir = os.path.dirname(os.path.abspath(__file__))
# Lùi ra thư mục cha, sau đó đi vào ingestion/data
data_dir = os.path.join(current_dir, '..', 'ingestion', 'data')

print(f"Đang tìm dữ liệu tại: {data_dir}")
print(f"Đang kết nối tới Kafka tại {KAFKA_BROKER}...")
=======
# Khởi tạo Producer
producer = KafkaProducer(
    bootstrap_servers=[KAFKA_BROKER],
    value_serializer=lambda x: json.dumps(x).encode('utf-8'),
    batch_size=16384,
    linger_ms=10 
)
>>>>>>> 2e65b52ad13febf25a0dacbff2843b1d53fedde8

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

<<<<<<< HEAD
def process_files():
    total_sent = 0
    total = 2
    # Chạy vòng lặp từ 1 đến 20
    for i in range(1, total):
        file_name = f"job_data_{i}.json"
        file_path = os.path.join(data_dir, file_name)

        # Kiểm tra file có tồn tại không
        if not os.path.exists(file_path):
            print(f"⚠️ Không tìm thấy file: {file_name} - Bỏ qua.")
            continue

        print(f"\n📂 Đang đọc file: {file_name}...")

        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                # Đọc toàn bộ nội dung file JSON
                records = json.load(f)
                
                # Giả sử file chứa một danh sách (list) các bản ghi
                if isinstance(records, list):
                    for record in records:
                        # Gửi từng bản ghi vào Kafka
                        producer.send(TOPIC_NAME, value=record)
                        total_sent += 1
                        
                        # (Tuỳ chọn) Sleep nhỏ để không bị spam log quá nhanh
                        # time.sleep(0.05) 
                    
                    print(f"   -> Đã gửi {len(records)} bản ghi từ {file_name}")
                else:
                    # Trường hợp file chỉ chứa 1 object JSON duy nhất
                    producer.send(TOPIC_NAME, value=records)
                    print(f"   -> Đã gửi 1 bản ghi từ {file_name}")
                    total_sent += 1

        except Exception as e:
            print(f"❌ Lỗi khi đọc file {file_name}: {e}")

    # Đảm bảo tất cả tin nhắn đã được đẩy đi hết trước khi đóng
    producer.flush()
    print("-" * 30)
    print(f"✅ Hoàn tất! Tổng cộng đã gửi {total_sent} bản ghi.")

if __name__ == "__main__":
    try:
        process_files()
    except KeyboardInterrupt:
        print("\nĐã dừng Producer.")
    finally:
        producer.close()
=======
# Chế độ 1: Nạp lịch sử (Batch) -> Topic: itjobs_history
def run_batch_mode(jobs):
    TOPIC = 'itjobs_history'
    print(f"[BATCH MODE] Đang nạp {len(jobs)} jobs vào topic '{TOPIC}'...")
    
    for i, job in enumerate(jobs):
        job['data_type'] = 'history'
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
            job['data_type'] = 'live'
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
>>>>>>> 2e65b52ad13febf25a0dacbff2843b1d53fedde8
