import json
import time
import sys
import os
import random
from kafka import KafkaProducer

KAFKA_BROKER = '192.168.56.104:30092' 
DATA_FOLDER = '../data'

# Khởi tạo Producer
producer = KafkaProducer(
    bootstrap_servers=[KAFKA_BROKER],
    value_serializer=lambda x: json.dumps(x).encode('utf-8'),
    batch_size=32768,
    linger_ms=50
)

def load_data_from_folder():
    """Đọc tất cả file JSON trong thư mục data và loại bỏ bản ghi không xác định"""
    all_jobs = []
    
    try:
        # Lấy đường dẫn tuyệt đối của thư mục data
        data_path = os.path.join(os.path.dirname(__file__), DATA_FOLDER)
        
        # Kiểm tra thư mục có tồn tại không
        if not os.path.exists(data_path):
            print(f"Thư mục '{data_path}' không tồn tại.")
            return []
        
        # Lấy danh sách tất cả file JSON trong thư mục
        json_files = [f for f in os.listdir(data_path) if f.endswith('.json')]
        
        if not json_files:
            print(f"Không tìm thấy file JSON nào trong thư mục '{data_path}'.")
            return []
        
        print(f"Tìm thấy {len(json_files)} file JSON trong thư mục data")
        
        # Đọc từng file JSON
        for json_file in json_files:
            file_path = os.path.join(data_path, json_file)
            print(f"Đang đọc file: {json_file}")
            
            try:
                with open(file_path, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                    
                    # Xử lý trường hợp data là dict hoặc list
                    if isinstance(data, dict):
                        data = [data]
                    
                    # Lọc bỏ các bản ghi có "Tên công việc" là "Không xác định"
                    valid_jobs = [
                        job for job in data 
                        if job.get('Tên công việc') != 'Không xác định'
                    ]
                    
                    skipped = len(data) - len(valid_jobs)
                    if skipped > 0:
                        print(f"  → Đã bỏ qua {skipped} bản ghi 'Không xác định' trong {json_file}")
                    
                    all_jobs.extend(valid_jobs)
                    print(f"  → Đã load {len(valid_jobs)} bản ghi hợp lệ từ {json_file}")
                    
            except json.JSONDecodeError as e:
                print(f"Lỗi đọc file JSON '{json_file}': {e}")
            except Exception as e:
                print(f"Lỗi xử lý file '{json_file}': {e}")
        
        print(f"\nTổng cộng: {len(all_jobs)} bản ghi hợp lệ từ {len(json_files)} file")
        return all_jobs
        
    except Exception as e:
        print(f"Lỗi khi đọc thư mục: {e}")
        return []

# Chế độ 1: Nạp lịch sử (Batch) -> Topic: itjobs_history
def run_batch_mode(jobs):
    TOPIC = 'itjobs_history'
    print(f"[BATCH MODE] Đang nạp {len(jobs)} jobs vào topic '{TOPIC}'...")
    
    for i, job in enumerate(jobs):
        job['ingest_time'] = str(time.time())
        producer.send(TOPIC, value=job)
        
        if (i + 1) % 500 == 0:
            print(f" -> Đã gửi {i + 1} jobs...")
            producer.flush() # Flush định kỳ để tránh quá tải bộ nhớ
            
    producer.flush()
    print(f"Đã nạp xong {len(jobs)} bản ghi vào lịch sử.")

# Chế độ 2: Giả lập Streaming -> Topic: itjobs_live
def run_streaming_mode(jobs):
    TOPIC = 'itjobs_live'
    print(f"[STREAM MODE] Đang giả lập dữ liệu vào topic '{TOPIC}'...")
    print("Chiến thuật: Gửi theo chùm (Burst) để tối ưu cho Spark Trigger 60s")
    
    # Cấu hình giả lập
    BURST_SIZE = 50
    SLEEP_TIME = 10 

    while True:
        if len(jobs) < BURST_SIZE:
            current_batch = jobs
        else:
            current_batch = random.sample(jobs, BURST_SIZE)

        print(f"--- Đang bắn chùm {len(current_batch)} bản ghi ---")
        
        for job in current_batch:
            job['ingest_time'] = str(time.time())
            producer.send(TOPIC, value=job)
        
        # Đẩy dữ liệu đi ngay lập tức
        producer.flush()
        
        print(f" -> Đã gửi xong {len(current_batch)} tin. Ngủ {SLEEP_TIME} giây...")
        time.sleep(SLEEP_TIME)

if __name__ == "__main__":
    data = load_data_from_folder()
    
    if not data:
        print("Không có dữ liệu để xử lý.")
        sys.exit(1)

    if len(sys.argv) > 1 and sys.argv[1] == 'batch':
        run_batch_mode(data)
    else:
        run_streaming_mode(data)