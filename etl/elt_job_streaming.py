import json
import pandas as pd
from minio import Minio
import re
import os
import time
from datetime import datetime, timezone

from dotenv import load_dotenv
from pymongo import MongoClient, UpdateOne
from pymongo.errors import BulkWriteError
# Tự động tạo job_id cho mỗi lần chạy
import uuid
job_id = str(uuid.uuid4())

# ================= LOAD ENV =================
load_dotenv()

# ================= CONSTANT =================
MAX_INT64 = 9_223_372_036_854_775_807  # dùng để biểu diễn "vô cùng"
POLL_INTERVAL = int(os.getenv("POLL_INTERVAL", "5"))  # seconds
PROCESSED_LOG = os.getenv("PROCESSED_LOG", "processed_files.txt")

# ================= MINIO CONFIG =================
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "192.168.56.103:30000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "admin")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "password123")

SOURCE_BUCKET = os.getenv("SOURCE_BUCKET", "bucket1")
SOURCE_FOLDER = os.getenv("SOURCE_FOLDER", "streaming/")  # Chỉ xử lý dữ liệu trong folder 'batch

# ================= MONGODB CONFIG (.env) =================
MONGO_URI = os.getenv("MONGODB_URI")
MONGO_DB = os.getenv("MONGODB_DB", "serving")
MONGO_COLL = os.getenv("MONGODB_COLLECTION", "jobs_realtime")

if not MONGO_URI:
    raise ValueError("Thiếu MONGODB_URI trong file .env (MongoDB Atlas URI).")

# ================= CLEAN FUNCTIONS =================
def clean_salary(salary_str):
    if pd.isna(salary_str) or str(salary_str).strip() == "" or salary_str == "Thoả thuận":
        return 0, MAX_INT64, "VND"

    s = str(salary_str).lower().replace(",", "").replace(".", "")
    currency = "USD" if ("usd" in s or "$" in s) else "VND"
    numbers = [int(n) for n in re.findall(r"\d+", s)]

    min_sal, max_sal = 0, 0
    if len(numbers) >= 2:
        min_sal, max_sal = numbers[0], numbers[1]
    elif len(numbers) == 1:
        if "tới" in s or "up to" in s:
            max_sal = numbers[0]
        elif "từ" in s:
            min_sal = numbers[0]
        else:
            max_sal = numbers[0]

    # Quy đổi "triệu" cho VND nếu số nhỏ
    if currency == "VND":
        if 0 < min_sal < 1000:
            min_sal *= 1_000_000
        if 0 < max_sal < 1000:
            max_sal *= 1_000_000

    # Nếu chỉ có "từ X" thì coi như không có trần
    if max_sal == 0 and min_sal > 0:
        max_sal = MAX_INT64

    return min_sal, max_sal, currency


def clean_experience(exp_str):
    """
    Trả về years_of_experience (int)
    - 'không yêu cầu' hoặc null => 0
    """
    if pd.isna(exp_str) or "không yêu cầu" in str(exp_str).lower():
        return 0
    numbers = re.findall(r"\d+", str(exp_str))
    return int(numbers[0]) if numbers else 0


# ================= PROCESSED FILE TRACKING =================
def load_processed_files():
    if not os.path.exists(PROCESSED_LOG):
        return set()
    with open(PROCESSED_LOG, "r", encoding="utf-8") as f:
        return set(line.strip() for line in f if line.strip())


def save_processed_file(file_name):
    with open(PROCESSED_LOG, "a", encoding="utf-8") as f:
        f.write(file_name + "\n")


# ================= JSON NORMALIZE =================
def extract_records(json_data):
    """
    Hỗ trợ 2 dạng:
      1) {"fullContent": [ ... ]}
      2) [ ... ]
    """
    if isinstance(json_data, dict) and "fullContent" in json_data and isinstance(json_data["fullContent"], list):
        return json_data["fullContent"]
    if isinstance(json_data, list):
        return json_data
    return []


def normalize_record(rec: dict) -> dict:
    """
    Chuẩn hoá 1 record sang schema EN + clean fields
    """
    # Nếu có trường id thì dùng làm job_id
    # job_id = rec.get("id", None)

    min_sal, max_sal, currency = clean_salary(rec.get("Mức lương"))
    yoe = clean_experience(rec.get("Kinh nghiệm"))

    out = {
        "job_id": job_id,
        "company": rec.get("Tên công ty"),
        "location": rec.get("Địa điểm"),

        "min_salary": int(min_sal) if min_sal is not None else 0,
        "max_salary": int(max_sal) if max_sal is not None else MAX_INT64,
        "currency": currency,

        "years_of_experience": int(yoe),

        "job_description": rec.get("Mô tả công việc"),
        "requirements": rec.get("Yêu cầu ứng viên"),
        "benefits": rec.get("Quyền lợi"),

        "workplace": rec.get("Địa điểm làm việc"),
        "working_time": rec.get("Thời gian làm việc"),
        "apply_method": rec.get("Cách thức ứng tuyển"),

        "raw_salary": rec.get("Mức lương"),
        "raw_experience": rec.get("Kinh nghiệm"),

        "ingested_at": datetime.now(timezone.utc),
    }
    return out


# ================= MONGO HELPERS =================
def connect_mongo():
    print("🧩 Kết nối MongoDB Atlas...")
    client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=8000)
    db = client[MONGO_DB]
    coll = db[MONGO_COLL]
    coll.create_index("job_id", unique=True)
    return client, coll


def upsert_many(coll, docs):
    """
    Upsert theo job_id:
      - tồn tại: update
      - chưa có: insert
    """
    ops = []
    for d in docs:
        if d.get("job_id") is None:
            continue
        ops.append(UpdateOne({"job_id": d["job_id"]}, {"$set": d}, upsert=True))

    if not ops:
        return {"matched": 0, "upserted": 0, "modified": 0}

    try:
        res = coll.bulk_write(ops, ordered=False)
        return {
            "matched": res.matched_count,
            "modified": res.modified_count,
            "upserted": len(res.upserted_ids) if res.upserted_ids else 0
        }
    except BulkWriteError as e:
        return {"error": "BulkWriteError", "details": e.details}


# ================= PROCESS SINGLE FILE =================
def process_file(minio_client, mongo_coll, file_name):
    print(f"File mới: {file_name}")

    resp = minio_client.get_object(SOURCE_BUCKET, file_name)
    content = resp.read()

    try:
        json_data = json.loads(content)
    except Exception as e:
        print(f"JSON parse lỗi: {e}")
        return

    records = extract_records(json_data)
    if not records:
        print("JSON không đúng cấu trúc (list hoặc dict.fullContent), bỏ qua")
        return

    docs = []
    for rec in records:
        if isinstance(rec, dict):
            doc = normalize_record(rec)
            if doc.get("job_id") is not None:
                docs.append(doc)

    if not docs:
        print("Không có record hợp lệ (thiếu job_id), bỏ qua")
        return

    stats = upsert_many(mongo_coll, docs)
    print(f"Upsert MongoDB: {stats} | records={len(docs)}")


# ================= MAIN STREAM LOOP =================
def main():
    # 1) MinIO
    print("Kết nối MinIO...")
    minio_client = Minio(
        MINIO_ENDPOINT,
        access_key=MINIO_ACCESS_KEY,
        secret_key=MINIO_SECRET_KEY,
        secure=False
    )

    # 2) MongoDB Atlas
    mongo_client, mongo_coll = connect_mongo()

    processed_files = load_processed_files()
    print(f"Đã xử lý trước đó: {len(processed_files)} file")
    print("BẮT ĐẦU REALTIME STREAMING MinIO -> MongoDB Atlas (0 → ∞)\n")

    try:
        while True:
            objects = minio_client.list_objects(
                SOURCE_BUCKET,
                prefix=SOURCE_FOLDER,
                recursive=True
            )

            for obj in objects:
                file_name = obj.object_name

                if not file_name.endswith(".json"):
                    continue

                if file_name in processed_files:
                    continue

                try:
                    process_file(minio_client, mongo_coll, file_name)
                    processed_files.add(file_name)
                    save_processed_file(file_name)
                except Exception as e:
                    print(f"Lỗi xử lý {file_name}: {e}")

            time.sleep(POLL_INTERVAL)

    finally:
        # đóng mongo client
        try:
            mongo_client.close()
        except Exception:
            pass


if __name__ == "__main__":
    main()
