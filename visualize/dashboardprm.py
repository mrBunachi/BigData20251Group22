import streamlit as st
import pandas as pd
from pymongo import MongoClient
import plotly.express as px
import time
import re
from datetime import datetime

# ================= CẤU HÌNH =================
MONGO_URI = "mongodb+srv://bigData:bigGroup22@bigdata.uaojt2r.mongodb.net/?retryWrites=true&w=majority"
DB_NAME = "serving"
COLLECTION_NAME = "jobs_realtime"

st.set_page_config(
    page_title="Real-time IT Jobs Monitor",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="expanded" # Tự động mở thanh bên
)

# ================= TỪ KHÓA SKILL (REGEX) =================
# Giữ nguyên danh sách xịn xò của bạn
SKILLS_MAPPING = {
    # --- Lập trình ---
    "Python": r"python", "Java": r"java\b", "JS/Node": r"javascript|js\b|node",
    "C/C++": r"\bc\+\+|\bc\b", "C#/.NET": r"c\#|\.net", "PHP": r"php", "Go": r"golang|go\b",
    
    # --- Web / Frontend ---
    "HTML/CSS": r"html|css", "React/Vue": r"react|vue", "Angular": r"angular",
    
    # --- Database ---
    "SQL": r"sql", "NoSQL/Mongo": r"mongodb|mongo|nosql",
    
    # --- Cloud / DevOps ---
    "AWS/Cloud": r"aws|cloud|azure", "Docker/K8s": r"docker|kubernetes|k8s",
    
    # --- Ngoại ngữ & Khác ---
    "Excel": r"excel", "English/IELTS": r"english|ielts|toeic", "Japanese/JLPT": r"japanese|jlpt|n[1-5]\b"
}

# ================= HÀM XỬ LÝ (BACKEND) =================
@st.cache_resource
def init_connection():
    return MongoClient(MONGO_URI)

def get_data():
    try:
        client = init_connection()
        db = client[DB_NAME]
        items = db[COLLECTION_NAME].find().sort("ingested_at", -1).limit(1000) # Lấy nhiều hơn chút
        df = pd.DataFrame(list(items))
        
        # Xử lý ngày tháng để vẽ biểu đồ streaming
        if not df.empty and 'ingested_at' in df.columns:
            df['ingested_at'] = pd.to_datetime(df['ingested_at'])
            
        return df
    except Exception as e:
        st.error(f"Lỗi kết nối: {e}")
        return pd.DataFrame()

def extract_level(text):
    """(MỚI) Tự động đoán Level (Junior/Senior) từ mô tả"""
    text = str(text).lower()
    if re.search(r'intern|thực tập|sinh viên', text): return 'Intern/Fresher'
    if re.search(r'fresher|mới tốt nghiệp', text): return 'Intern/Fresher'
    if re.search(r'senior|lead|trưởng nhóm|quản lý', text): return 'Senior/Lead'
    if re.search(r'junior|nhân viên', text): return 'Junior/Mid'
    return 'Junior/Mid' # Mặc định

def extract_skills(df):
    """Hàm quét skill cũ của bạn (giữ nguyên logic)"""
    if df.empty: return pd.DataFrame()
    cols_to_scan = ['job_description', 'requirements', 'Mô tả công việc', 'Yêu cầu ứng viên']
    combined_text = pd.Series([""] * len(df))
    for col in cols_to_scan:
        if col in df.columns: combined_text += " " + df[col].fillna("").astype(str)
            
    skill_counts = {}
    for skill, pattern in SKILLS_MAPPING.items():
        count = combined_text.str.contains(pattern, flags=re.IGNORECASE, regex=True).sum()
        if count > 0: skill_counts[skill] = count
            
    return pd.DataFrame(list(skill_counts.items()), columns=['Skill', 'Count']).sort_values('Count', ascending=False)

# ================= GIAO DIỆN DASHBOARD =================

# --- 1. SIDEBAR BỘ LỌC (MỚI) ---
st.sidebar.title("🎛️ Bộ Lọc Dữ Liệu")
df_raw = get_data()

if not df_raw.empty:
    # Tạo cột Level
    df_raw['Level'] = df_raw.apply(lambda x: extract_level(str(x.get('job_title', '')) + " " + str(x.get('job_description', ''))), axis=1)
    
    # Chọn thành phố
    cities = ['Tất cả'] + list(df_raw['location'].unique()) if 'location' in df_raw.columns else ['Tất cả']
    selected_city = st.sidebar.selectbox("Thành phố:", cities)
    
    # Chọn Level
    levels = ['Tất cả'] + list(df_raw['Level'].unique())
    selected_level = st.sidebar.selectbox("Level:", levels)
    
    # Lọc dữ liệu
    df = df_raw.copy()
    if selected_city != 'Tất cả': df = df[df['location'] == selected_city]
    if selected_level != 'Tất cả': df = df[df['Level'] == selected_level]
    
    st.sidebar.success(f"Hiển thị: {len(df)} job")
    if st.sidebar.button('🔄 Refresh'): st.rerun()
else:
    df = pd.DataFrame()
    st.sidebar.warning("Chưa có dữ liệu từ Spark!")

# --- 2. PHẦN CHÍNH ---
st.title("🚀 Real-time IT Jobs Dashboard")
st.markdown(f"Hệ thống giám sát tuyển dụng IT • **{selected_city}** • **{selected_level}**")

if not df.empty:
    # --- KPI (GIỮ NGUYÊN + CẢI TIẾN) ---
    col1, col2, col3, col4 = st.columns(4)
    col1.metric("Tổng tin", len(df))
    
    # Tính lương trung bình thay vì Max (nhìn khoa học hơn)
    avg_sal = 0
    if 'max_salary' in df.columns and 'currency' in df.columns:
        df_sal = df[(df['currency']=='VND') & (df['max_salary']>0)]
        if not df_sal.empty: avg_sal = df_sal['max_salary'].mean()
    col2.metric("Lương TB (VND)", f"{avg_sal:,.0f} đ")
    
    # Công ty mới nhất
    latest_comp = df.iloc[0].get('company', 'N/A')
    col3.metric("Công ty mới nhất", latest_comp)

    # Thời gian cập nhật
    last_update = df['ingested_at'].max().strftime('%H:%M:%S') if 'ingested_at' in df.columns else "N/A"
    col4.metric("Cập nhật lúc", last_update)

    st.divider()

    # --- BIỂU ĐỒ STREAMING (MỚI - RẤT QUAN TRỌNG CHO ĐỒ ÁN) ---
    st.subheader("📈 Xu hướng tin tuyển dụng (Streaming)")
    if 'ingested_at' in df.columns:
        # Nhóm theo giờ để vẽ biểu đồ đường
        df_trend = df.set_index('ingested_at').resample('H').size().reset_index(name='count')
        fig_trend = px.area(df_trend, x='ingested_at', y='count', markers=True, 
                            title="Lượng tin đổ về theo thời gian", color_discrete_sequence=['#00CC96'])
        st.plotly_chart(fig_trend, use_container_width=True)

    # --- PHÂN TÍCH SKILL & LEVEL (KẾT HỢP CŨ & MỚI) ---
    c1, c2 = st.columns([2, 1])
    
    with c1:
        st.subheader("🛠️ Top Skill Hot Nhất")
        df_skills = extract_skills(df)
        if not df_skills.empty:
            fig_skill = px.bar(df_skills.head(12), x='Skill', y='Count', text='Count', 
                               color='Count', color_continuous_scale='Viridis')
            st.plotly_chart(fig_skill, use_container_width=True)
            
    with c2:
        st.subheader("🎓 Tỷ lệ Level")
        fig_lvl = px.pie(
            df,
            names='Level',
            hole=0.5,
            title="Tỷ lệ Level"
        )
        st.plotly_chart(fig_lvl, use_container_width=True)

    # --- PHÂN TÍCH LƯƠNG & ĐỊA ĐIỂM (GIỮ LẠI CÁI CŨ BẠN THÍCH) ---
    c3, c4 = st.columns(2)
    with c3:
        st.subheader("📍 Địa điểm")
        if 'location' in df.columns:
            loc_counts = df['location'].value_counts().reset_index()
            loc_counts.columns = ['location', 'count']
            st.plotly_chart(px.pie(loc_counts, values='count', names='location', hole=0.4), use_container_width=True)
            
    with c4:
        st.subheader("💰 Phân bố Lương (Box Plot)")
        if not df_sal.empty:
            # Box plot xịn hơn Histogram cũ
            fig_box = px.box(df_sal, x='Level', y='max_salary', points="all", color='Level')
            st.plotly_chart(fig_box, use_container_width=True)
        else:
            st.info("Chưa có dữ liệu lương VND.")

    # --- DỮ LIỆU CHI TIẾT ---
    with st.expander("📋 Xem dữ liệu chi tiết"):
        st.dataframe(df.head(50), use_container_width=True)

else:
    st.warning("⚠️ Đang chờ Spark Streaming...")

time.sleep(30) # Refresh mỗi 30s
st.rerun()