import json
import pandas as pd
from minio import Minio  # <--- Đã sửa: Minio (io viết thường)
from io import BytesIO
import re
import os

# ================= CẤU HÌNH KẾT NỐI MINIO =================
# LƯU Ý: Thay đổi IP này thành IP máy ảo của bạn
MINIO_ENDPOINT = "192.168.56.103:30000" 
MINIO_ACCESS_KEY = "admin"
MINIO_SECRET_KEY = "password123"

SOURCE_BUCKET = "bucket1"       # Bucket chứa dữ liệu thô (JSON)
DEST_BUCKET = "bucket2"   # Bucket chứa dữ liệu sạch (Parquet)

# ================= HÀM LÀM SẠCH DỮ LIỆU =================
def clean_salary(salary_str):
    """Hàm tách lương thành min, max và đơn vị tiền tệ"""
    if pd.isna(salary_str) or salary_str == "Thoả thuận":
        return 0, 0, "VND"
    
    s = str(salary_str).lower().replace(",", "").replace(".", "")
    currency = "USD" if "usd" in s or "$" in s else "VND"
    
    # Tìm tất cả các số trong chuỗi
    numbers = [int(n) for n in re.findall(r'\d+', s)]
    
    min_sal, max_sal = 0, 0
    
    if len(numbers) == 2:
        min_sal, max_sal = numbers[0], numbers[1]
    elif len(numbers) == 1:
        if "tới" in s or "up to" in s:
            max_sal = numbers[0]
        elif "từ" in s:
            min_sal = numbers[0]
        else:
            max_sal = numbers[0]
            
    # Quy đổi triệu đồng về đơn vị đồng
    if currency == "VND":
        if min_sal > 0 and min_sal < 1000: min_sal *= 1000000
        if max_sal > 0 and max_sal < 1000: max_sal *= 1000000
            
    return min_sal, max_sal, currency

def clean_experience(exp_str):
    """Hàm lấy số năm kinh nghiệm"""
    if pd.isna(exp_str) or "không yêu cầu" in str(exp_str).lower():
        return 0
    numbers = re.findall(r'\d+', str(exp_str))
    return int(numbers[0]) if numbers else 0

# ================= CHƯƠNG TRÌNH CHÍNH =================
def main():
    print(f"🔌 Đang kết nối tới MinIO: {MINIO_ENDPOINT}...")
    
    # <--- Đã sửa: Minio (io viết thường)
    client = Minio(
        MINIO_ENDPOINT,
        access_key=MINIO_ACCESS_KEY,
        secret_key=MINIO_SECRET_KEY,
        secure=False
    )

    # Đảm bảo bucket đích tồn tại
    if not client.bucket_exists(DEST_BUCKET):
        client.make_bucket(DEST_BUCKET)
        print(f"✅ Đã tạo bucket mới: {DEST_BUCKET}")

    # Lấy danh sách file trong bucket nguồn
    try:
        objects = client.list_objects(SOURCE_BUCKET)
        found_file = False
        
        for obj in objects:
            file_name = obj.object_name
            # Chỉ xử lý file .json
            if not file_name.endswith('.json'): 
                continue
            
            found_file = True
            print(f"\n📄 Tìm thấy file: {file_name}. Đang xử lý...")

            # 1. EXTRACT: Đọc file từ MinIO
            response = client.get_object(SOURCE_BUCKET, file_name)
            file_content = response.read()
            
            # Xử lý trường hợp file JSON có cấu trúc đặc biệt (như file bạn gửi)
            try:
                json_data = json.loads(file_content)
                # Nếu file bạn gửi có cấu trúc { "type":..., "fullContent": [...] }
                if isinstance(json_data, dict) and "fullContent" in json_data:
                    data_list = json_data["fullContent"]
                elif isinstance(json_data, list):
                    data_list = json_data
                else:
                    print(f"⚠️ Cấu trúc JSON trong file {file_name} không hỗ trợ.")
                    continue
            except Exception as e:
                print(f"❌ Lỗi đọc JSON file {file_name}: {e}")
                continue

            # 2. TRANSFORM: Làm sạch bằng Pandas
            df = pd.DataFrame(data_list)
            
            # Áp dụng hàm làm sạch
            salary_data = df['Mức lương'].apply(lambda x: clean_salary(x))
            df['min_salary'] = [x[0] for x in salary_data]
            df['max_salary'] = [x[1] for x in salary_data]
            df['currency'] = [x[2] for x in salary_data]
            
            df['years_of_experience'] = df['Kinh nghiệm'].apply(clean_experience)
            
            # Đổi tên cột sang tiếng Anh
            df = df.rename(columns={
                "Tên công ty": "company", 
                "Địa điểm": "location",
                "Mô tả công việc": "job_description", 
                "Yêu cầu ứng viên": "requirements",
                "Quyền lợi": "benefits",
                "Mức lương": "raw_salary",
                "Kinh nghiệm": "raw_experience"
            })
            
            # Chọn các cột cần thiết để lưu
            final_df = df[[
                'company', 'location', 'min_salary', 'max_salary', 'currency', 
                'years_of_experience', 'job_description', 'requirements', 'benefits'
            ]]
            
            print(f"   ✅ Đã chuẩn hóa {len(final_df)} dòng dữ liệu.")

            # 3. LOAD: Ghi file Parquet lên Bucket đích
            # Đổi tên file: job_data.json -> job_data.parquet
            new_file_name = file_name.replace(".json", ".parquet")
            
            # Ghi vào bộ nhớ đệm (RAM)
            parquet_buffer = BytesIO()
            final_df.to_parquet(parquet_buffer, index=False)
            parquet_buffer.seek(0)
            
            # Upload
            client.put_object(
                DEST_BUCKET,
                new_file_name,
                data=parquet_buffer,
                length=parquet_buffer.getbuffer().nbytes,
                content_type="application/octet-stream"
            )
            print(f"   🚀 Đã đẩy file sạch lên: {DEST_BUCKET}/{new_file_name}")

        if not found_file:
            print(f"⚠️ Không tìm thấy file .json nào trong bucket '{SOURCE_BUCKET}'. Hãy upload file lên MinIO trước.")

    except Exception as e:
        print(f"❌ Lỗi kết nối hoặc xử lý: {e}")

if __name__ == "__main__":
    main()