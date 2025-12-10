import json
import pandas as pd
import re
import os

def clean_text(text):
    """Làm sạch văn bản: xóa khoảng trắng thừa, ký tự xuống dòng."""
    if not isinstance(text, str):
        return ""
    # Xóa ký tự xuống dòng và khoảng trắng thừa
    text = re.sub(r'[\r\n\t]+', ' ', text)
    return text.strip()

def parse_salary(salary_str):
    """
    Phân tích chuỗi lương thành min, max và đơn vị tiền tệ.
    Ví dụ: '15 - 20 triệu' -> (15000000, 20000000, 'VND')
           'Tới 1,500 USD' -> (0, 1500, 'USD')
    """
    if not isinstance(salary_str, str) or salary_str.lower() == "thoả thuận":
        return None, None, "VND"

    currency = "USD" if "USD" in salary_str.upper() else "VND"
    
    # Xóa các ký tự không phải số và dấu gạch nối
    # Giữ lại số, dấu chấm, dấu phẩy, dấu gạch ngang
    clean_str = re.sub(r'[^\d\.\,\-]', '', salary_str)
    
    # Tách khoảng lương (ví dụ: 15-20)
    parts = re.split(r'-', clean_str)
    
    try:
        if len(parts) == 2:
            min_sal = float(parts[0].replace(',', '').replace('.', ''))
            max_sal = float(parts[1].replace(',', '').replace('.', ''))
        elif len(parts) == 1 and parts[0]:
             # Trường hợp "Tới X" hoặc "Từ X" -> Xử lý đơn giản gán vào max hoặc min tùy logic
             # Ở đây tạm gán cho max nếu chuỗi gốc có chữ "Tới", min nếu "Từ"
             val = float(parts[0].replace(',', '').replace('.', ''))
             if "tới" in salary_str.lower() or "upto" in salary_str.lower():
                 min_sal, max_sal = 0, val
             else:
                 min_sal, max_sal = val, 0 # Hoặc val, val nếu muốn
        else:
             return None, None, currency

        # Chuẩn hóa đơn vị triệu đồng
        if currency == "VND":
             # Logic đoán đơn vị: nếu số nhỏ < 1000 khả năng là "triệu"
             # Cần logic chặt chẽ hơn tùy dữ liệu thực tế
             if max_sal > 0 and max_sal < 1000: 
                 max_sal *= 1000000
             if min_sal > 0 and min_sal < 1000:
                 min_sal *= 1000000
                 
        return min_sal, max_sal, currency
    except:
        return None, None, currency

def parse_experience(exp_str):
    """Chuyển đổi kinh nghiệm thành số năm (min_years)."""
    if not isinstance(exp_str, str):
        return 0
    
    exp_str = exp_str.lower()
    if "không yêu cầu" in exp_str or "dưới 1 năm" in exp_str:
        return 0
    
    # Tìm số đầu tiên trong chuỗi (ví dụ: "3 năm" -> 3)
    match = re.search(r'\d+', exp_str)
    if match:
        return int(match.group())
    return 0

def process_job_data(file_paths):
    all_jobs = []
    
    for file_path in file_paths:
        if not os.path.exists(file_path):
            print(f"File not found: {file_path}")
            continue
            
        with open(file_path, 'r', encoding='utf-8') as f:
            try:
                data = json.load(f)
                # Xử lý trường hợp file JSON có cấu trúc khác nhau (list hoặc dict)
                if isinstance(data, list):
                    all_jobs.extend(data)
                elif isinstance(data, dict) and 'fullContent' in data: # Cấu trúc từ ví dụ của bạn
                     all_jobs.extend(data['fullContent'])
            except json.JSONDecodeError:
                print(f"Error reading JSON file: {file_path}")

    # Tạo DataFrame
    df = pd.DataFrame(all_jobs)
    
    # 1. Làm sạch các trường văn bản cơ bản
    text_cols = ['Tên công ty', 'Địa điểm', 'Mô tả công việc', 'Yêu cầu ứng viên', 'Quyền lợi', 'Địa điểm làm việc']
    for col in text_cols:
        if col in df.columns:
            df[col] = df[col].apply(clean_text)

    # 2. Chuẩn hóa Lương
    if 'Mức lương' in df.columns:
        salary_data = df['Mức lương'].apply(parse_salary)
        df['salary_min'] = [x[0] for x in salary_data]
        df['salary_max'] = [x[1] for x in salary_data]
        df['currency'] = [x[2] for x in salary_data]

    # 3. Chuẩn hóa Kinh nghiệm
    if 'Kinh nghiệm' in df.columns:
        df['exp_years'] = df['Kinh nghiệm'].apply(parse_experience)

    # 4. Chuẩn hóa Địa điểm (Lấy Thành phố chính)
    if 'Địa điểm' in df.columns:
        # Giả sử định dạng "Hà Nội", "Hồ Chí Minh", hoặc chuỗi dài
        # Lấy từ đầu tiên hoặc logic map cụ thể
        df['city'] = df['Địa điểm'].apply(lambda x: x.split(':')[0].strip() if x else "Unknown")
        # Chuẩn hóa tên thành phố (ví dụ: Ha Noi -> Hà Nội)
        df['city'] = df['city'].replace({
            'Ha Noi': 'Hà Nội', 'Ho Chi Minh': 'Hồ Chí Minh', 'TP HCM': 'Hồ Chí Minh'
        })

    # 5. Xử lý dữ liệu trùng lặp (nếu cần)
    df.drop_duplicates(subset=['Tên công ty', 'Mô tả công việc'], inplace=True)
    
    return df

# --- CHẠY CHƯƠNG TRÌNH ---
# Danh sách các file bạn đã upload
files = ['job_data_1.json', 'job_data_2.json', 'job_data_3.json', 'job_data_4.json', 'job_data_5.json', 
         'job_data_6.json', 'job_data_7.json', 'job_data_8.json', 'job_data_9.json', 'job_data_10.json']

# Xử lý dữ liệu
df_clean = process_job_data(files)

# Xem kết quả
print(f"Tổng số bản ghi: {len(df_clean)}")
print(df_clean[['Tên công ty', 'salary_min', 'salary_max', 'currency', 'exp_years', 'city']].head())

# --- XUẤT DỮ LIỆU CHO BIG DATA ---

# 1. Xuất ra CSV (Dễ đọc, phổ biến)
df_clean.to_csv('cleaned_jobs.csv', index=False, encoding='utf-8-sig')

# 2. Xuất ra Parquet (Tối ưu cho Big Data - Spark, Hadoop, AWS Athena...)
# Parquet nén tốt hơn và giữ được schema dữ liệu
try:
    df_clean.to_parquet('cleaned_jobs.parquet', index=False)
    print("Đã xuất file Parquet thành công.")
except Exception as e:
    print(f"Lỗi xuất Parquet (cần cài pyarrow/fastparquet): {e}")

# 3. Xuất ra JSON Lines (Mỗi dòng là 1 object JSON - Thích hợp cho log processing/NoSQL)
df_clean.to_json('cleaned_jobs.jsonl', orient='records', lines=True, force_ascii=False)