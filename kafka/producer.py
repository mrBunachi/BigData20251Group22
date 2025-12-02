import json
import time
import os
from kafka import KafkaProducer

# 1. Cấu hình Kafka
KAFKA_BROKER = '192.168.56.104:30092' 
TOPIC_NAME = 'test'

# 2. Xác định đường dẫn thư mục chứa data
# Lấy đường dẫn của file code hiện tại (trong folder kafka)
current_dir = os.path.dirname(os.path.abspath(__file__))
# Lùi ra thư mục cha, sau đó đi vào ingestion/data
data_dir = os.path.join(current_dir, '..', 'ingestion', 'data')

print(f"Đang tìm dữ liệu tại: {data_dir}")
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