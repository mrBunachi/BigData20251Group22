import json
import pandas as pd
from minio import Minio
from io import BytesIO
import re
import os

# ================= CẤU HÌNH KẾT NỐI MINIO =================
# Cổng NodePort bạn đã tìm được
MINIO_ENDPOINT = "192.168.56.103:30000" 
MINIO_ACCESS_KEY = "admin"
MINIO_SECRET_KEY = "password123"

# TÊN BUCKET CỦA BẠN
SOURCE_BUCKET = "bucket1"       # Chứa data thô
DEST_BUCKET = "bucket2"         # Chứa data sạch
SOURCE_FOLDER = "batch/"        # Chỉ xử lý dữ liệu trong folder 'batch'

# ================= HÀM LÀM SẠCH (GIỮ NGUYÊN) =================
def clean_salary(salary_str):
    if pd.isna(salary_str) or salary_str == "Thoả thuận":
        return 0, 0, "VND"
    s = str(salary_str).lower().replace(",", "").replace(".", "")
    currency = "USD" if "usd" in s or "$" in s else "VND"
    numbers = [int(n) for n in re.findall(r'\d+', s)]
    min_sal, max_sal = 0, 0
    if len(numbers) == 2:
        min_sal, max_sal = numbers[0], numbers[1]
    elif len(numbers) == 1:
        if "tới" in s or "up to" in s: max_sal = numbers[0]
        elif "từ" in s: min_sal = numbers[0]
        else: max_sal = numbers[0]
    if currency == "VND":
        if min_sal > 0 and min_sal < 1000: min_sal *= 1000000
        if max_sal > 0 and max_sal < 1000: max_sal *= 1000000
    return min_sal, max_sal, currency

def clean_experience(exp_str):
    if pd.isna(exp_str) or "không yêu cầu" in str(exp_str).lower():
        return 0
    numbers = re.findall(r'\d+', str(exp_str))
    return int(numbers[0]) if numbers else 0

# ================= CHƯƠNG TRÌNH CHÍNH =================
def main():
    print(f"🔌 Đang kết nối tới MinIO: {MINIO_ENDPOINT}...")
    client = Minio(
        MINIO_ENDPOINT,
        access_key=MINIO_ACCESS_KEY,
        secret_key=MINIO_SECRET_KEY,
        secure=False
    )

    # Đảm bảo bucket đích (bucket2) tồn tại
    if not client.bucket_exists(DEST_BUCKET):
        client.make_bucket(DEST_BUCKET)
        print(f"✅ Đã tạo bucket đích: {DEST_BUCKET}")

    try:
        # Lấy danh sách file CHỈ TRONG FOLDER 'batch/' của bucket1
        print(f"📂 Đang quét thư mục '{SOURCE_FOLDER}' trong {SOURCE_BUCKET}...")
        objects = client.list_objects(SOURCE_BUCKET, prefix=SOURCE_FOLDER, recursive=True)
        
        found_file = False
        for obj in objects:
            file_name = obj.object_name # Ví dụ: batch/job_data_5.json
            
            if not file_name.endswith('.json'): 
                continue
            
            found_file = True
            print(f"\n📄 Tìm thấy file: {file_name}. Đang xử lý...")

            # 1. EXTRACT
            response = client.get_object(SOURCE_BUCKET, file_name)
            file_content = response.read()
            
            try:
                json_data = json.loads(file_content)
                # Xử lý cấu trúc JSON của bạn
                if isinstance(json_data, dict) and "fullContent" in json_data:
                    data_list = json_data["fullContent"]
                elif isinstance(json_data, list):
                    data_list = json_data
                else:
                    print(f"⚠️ Cấu trúc JSON lạ, bỏ qua.")
                    continue
            except Exception as e:
                print(f"❌ Lỗi đọc JSON: {e}")
                continue

            # 2. TRANSFORM
            df = pd.DataFrame(data_list)
            
            salary_data = df['Mức lương'].apply(lambda x: clean_salary(x))
            df['min_salary'] = [x[0] for x in salary_data]
            df['max_salary'] = [x[1] for x in salary_data]
            df['currency'] = [x[2] for x in salary_data]
            df['years_of_experience'] = df['Kinh nghiệm'].apply(clean_experience)
            
            df = df.rename(columns={
                "Tên công ty": "company", "Địa điểm": "location",
                "Mô tả công việc": "job_description", "Yêu cầu ứng viên": "requirements",
                "Quyền lợi": "benefits", "Mức lương": "raw_salary", "Kinh nghiệm": "raw_experience"
            })
            
            final_df = df[['company', 'location', 'min_salary', 'max_salary', 'currency', 'years_of_experience', 'job_description', 'requirements', 'benefits']]
            
            print(f"   ✅ Đã chuẩn hóa {len(final_df)} dòng dữ liệu.")

            # 3. LOAD (Giữ nguyên cấu trúc thư mục sang bucket2)
            # batch/job.json -> batch/job.parquet
            new_file_name = file_name.replace(".json", ".parquet")
            
            parquet_buffer = BytesIO()
            final_df.to_parquet(parquet_buffer, index=False)
            parquet_buffer.seek(0)
            
            client.put_object(
                DEST_BUCKET,
                new_file_name,
                data=parquet_buffer,
                length=parquet_buffer.getbuffer().nbytes,
                content_type="application/octet-stream"
            )
            print(f"   🚀 Đã đẩy sang: {DEST_BUCKET}/{new_file_name}")

        if not found_file:
            print(f"⚠️ Không tìm thấy file JSON nào trong '{SOURCE_BUCKET}/{SOURCE_FOLDER}'. Hãy kiểm tra lại MinIO.")

    except Exception as e:
        print(f"❌ Lỗi: {e}")

if __name__ == "__main__":
    main()
