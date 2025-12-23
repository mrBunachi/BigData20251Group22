import streamlit as st
import pandas as pd
import plotly.express as px
import re
import os

# ================= 1. CẤU HÌNH KẾT NỐI MINIO =================
# Thông tin kết nối MinIO
MINIO_ENDPOINT = "http://192.168.56.103:30000" # Lưu ý phải có http://
MINIO_ACCESS_KEY = "admin"
MINIO_SECRET_KEY = "password123"
BUCKET_NAME = "bucket2" # <--- THAY TÊN BUCKET CỦA BẠN VÀO ĐÂY (VD: raw, bronze, silver...)
FOLDER_PATH = "batch" # <--- THAY ĐƯỜNG DẪN FOLDER CHỨA FILE PARQUET (nếu có)

# Cấu hình Pandas để đọc S3/MinIO
storage_options = {
    "key": MINIO_ACCESS_KEY,
    "secret": MINIO_SECRET_KEY,
    "client_kwargs": {
        "endpoint_url": MINIO_ENDPOINT
    }
}

st.set_page_config(
    page_title="Batch Data Analytics (MinIO)",
    page_icon="🗄️",
    layout="wide"
)

# ================= 2. CÁC HÀM XỬ LÝ (TÁI SỬ DỤNG TỪ DASHBOARD CŨ) =================
SKILLS_MAPPING = {
    "Python": r"python", "Java": r"java\b", "JS/Node": r"javascript|js\b|node",
    "SQL": r"sql", "NoSQL": r"mongodb|mongo|nosql",
    "Cloud/DevOps": r"aws|cloud|docker|kubernetes",
    "Excel": r"excel", "English": r"english|toeic|ielts"
}

@st.cache_data(ttl=300) # Cache dữ liệu 5 phút để đỡ phải load lại Parquet liên tục
def load_batch_data():
    """Đọc toàn bộ file Parquet từ MinIO"""
    path = f"s3://{BUCKET_NAME}/{FOLDER_PATH}"
    try:
        # Pandas hỗ trợ đọc thẳng từ S3 protocol nhờ s3fs
        # Nó sẽ tự động merge tất cả các file parquet trong folder lại thành 1 DataFrame
        df = pd.read_parquet(path, storage_options=storage_options)
        
        # Xử lý ngày tháng nếu có
        if 'ingested_at' in df.columns:
            df['ingested_at'] = pd.to_datetime(df['ingested_at'])
            
        return df
    except Exception as e:
        st.error(f"❌ Lỗi đọc dữ liệu từ MinIO: {e}")
        st.info("Gợi ý: Kiểm tra lại tên Bucket, Folder và quyền truy cập.")
        return pd.DataFrame()

def extract_skills(df):
    if df.empty: return pd.DataFrame()
    cols = [c for c in df.columns if c in ['job_description', 'requirements', 'Mô tả công việc']]
    if not cols: return pd.DataFrame()
    
    text = df[cols].fillna("").astype(str).agg(' '.join, axis=1)
    
    counts = {}
    for skill, regex in SKILLS_MAPPING.items():
        counts[skill] = text.str.contains(regex, flags=re.IGNORECASE, regex=True).sum()
        
    return pd.DataFrame(list(counts.items()), columns=['Skill', 'Count']).sort_values('Count', ascending=False)

# ================= 3. GIAO DIỆN DASHBOARD BATCH =================
st.title("🗄️ Historical Data Analytics")
st.markdown(f"**Nguồn dữ liệu:** MinIO Parquet (`{BUCKET_NAME}/{FOLDER_PATH}`) • **Batch Processing**")

if st.button('🔄 Reload Dữ liệu MinIO'):
    st.cache_data.clear()
    st.rerun()

df = load_batch_data()

if not df.empty:
    # --- ROW 1: TỔNG QUAN DỮ LIỆU LỊCH SỬ ---
    c1, c2, c3 = st.columns(3)
    c1.metric("Tổng số bản ghi (Batch)", f"{len(df):,}")
    
    # Tính tổng lương trung bình lịch sử
    avg_sal = 0
    if 'max_salary' in df.columns and 'currency' in df.columns:
        df_sal = df[(df['currency']=='VND') & (df['max_salary']>0)]
        if not df_sal.empty: avg_sal = df_sal['max_salary'].mean()
    c2.metric("Lương TB Lịch sử", f"{avg_sal:,.0f} đ")
    
    c3.metric("Số lượng cột (Features)", len(df.columns))

    st.divider()

    # --- ROW 2: PHÂN TÍCH SKILL TRÊN TẬP DỮ LIỆU LỚN ---
    col_left, col_right = st.columns([2, 1])
    
    with col_left:
        st.subheader("📊 Xu hướng Kỹ năng (Toàn bộ dữ liệu)")
        df_skills = extract_skills(df)
        if not df_skills.empty:
            fig_skill = px.bar(df_skills, x='Skill', y='Count', text='Count',
                               color='Count', color_continuous_scale='Magma',
                               title="Tần suất xuất hiện Skill trong kho dữ liệu")
            st.plotly_chart(fig_skill, use_container_width=True)
            
    with col_right:
        st.subheader("🏢 Top Công ty Tuyển dụng")
        if 'company' in df.columns:
            top_co = df['company'].value_counts().head(10).reset_index()
            top_co.columns = ['Company', 'Count']
            st.dataframe(top_co, use_container_width=True, hide_index=True)

    # --- ROW 3: PHÂN TÍCH LƯƠNG CHI TIẾT ---
    st.subheader("💰 Phân bố mức lương lịch sử")
    if not df_sal.empty:
        # Histogram cho thấy phân phối lương chuẩn hơn
        fig_hist = px.histogram(df_sal, x="max_salary", nbins=50, 
                                title="Phổ lương tuyển dụng (VND)",
                                labels={'max_salary': 'Mức lương'}, color_discrete_sequence=['#636EFA'])
        st.plotly_chart(fig_hist, use_container_width=True)
    else:
        st.info("Không đủ dữ liệu lương để vẽ biểu đồ.")

    # --- ROW 4: XEM DỮ LIỆU THÔ ---
    with st.expander("📂 Xem dữ liệu Parquet thô"):
        st.dataframe(df.head(100), use_container_width=True)

else:
    st.warning("⚠️ Chưa đọc được dữ liệu. Hãy kiểm tra kết nối MinIO!")