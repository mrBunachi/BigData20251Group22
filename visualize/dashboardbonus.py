import streamlit as st
import pandas as pd
from pymongo import MongoClient
import plotly.express as px
import time
import re

# ================= CẤU HÌNH =================
MONGO_URI = "mongodb+srv://bigData:bigGroup22@bigdata.uaojt2r.mongodb.net/?retryWrites=true&w=majority"
DB_NAME = "serving"
COLLECTION_NAME = "jobs_realtime"

st.set_page_config(
    page_title="Real-time IT Jobs Monitor",
    page_icon="📊",
    layout="wide"
)

# ================= TỪ KHÓA SKILL CẦN TÌM (REGEX) =================
# Danh sách này giúp bắt dính cả chữ hoa, chữ thường, viết tắt
SKILLS_MAPPING = {
    # --- Lập trình ---
    "Python": r"python",
    "Java": r"java\b", 
    "JavaScript": r"javascript|js\b",
    "TypeScript": r"typescript|ts\b",
    "C/C++": r"\bc\+\+|\bc\b",
    "C# / .NET": r"c\#|\.net",
    "PHP": r"php",
    "Go": r"golang|go\b",
    
    # --- Web / Frontend ---
    "HTML/CSS": r"html|css",
    "React": r"react",
    "Angular": r"angular",
    "Vue": r"vue",
    "NodeJS": r"node\.?js",
    
    # --- Database ---
    "SQL": r"sql",
    "MySQL": r"mysql",
    "PostgreSQL": r"postgresql|postgres",
    "MongoDB": r"mongodb|mongo",
    
    # --- Công cụ / Cloud ---
    "Excel": r"excel",
    "AWS": r"aws|amazon web services",
    "Docker": r"docker",
    "Kubernetes": r"kubernetes|k8s",
    "Git": r"git",
    "Linux": r"linux",
    
    # --- Ngoại ngữ & Chứng chỉ ---
    "English": r"english|tiếng anh",
    "Japanese": r"japanese|tiếng nhật",
    "IELTS": r"ielts",
    "JLPT (N1-N5)": r"jlpt|n[1-5]\b",
    "TOEIC": r"toeic"
}

# ================= HÀM XỬ LÝ =================
@st.cache_resource
def init_connection():
    return MongoClient(MONGO_URI)

def get_data():
    try:
        client = init_connection()
        db = client[DB_NAME]
        items = db[COLLECTION_NAME].find().sort("ingested_at", -1).limit(500)
        items = list(items)
        return pd.DataFrame(items)
    except Exception as e:
        st.error(f"Lỗi kết nối: {e}")
        return pd.DataFrame()

def extract_skills(df):
    """Hàm quét skill từ mô tả công việc"""
    if df.empty: return pd.DataFrame()
    
    # Gộp các cột text lại để tìm cho kỹ (job_description + requirements)
    cols_to_scan = ['job_description', 'requirements', 'Mô tả công việc', 'Yêu cầu ứng viên']
    combined_text = pd.Series([""] * len(df))
    
    for col in cols_to_scan:
        if col in df.columns:
            combined_text += " " + df[col].fillna("").astype(str)
            
    skill_counts = {}
    for skill, pattern in SKILLS_MAPPING.items():
        count = combined_text.str.contains(pattern, flags=re.IGNORECASE, regex=True).sum()
        if count > 0: skill_counts[skill] = count
            
    return pd.DataFrame(list(skill_counts.items()), columns=['Skill', 'Count']).sort_values('Count', ascending=False)

# ================= GIAO DIỆN DASHBOARD =================
st.title("🚀 Real-time IT Jobs Dashboard")
st.markdown("Hệ thống giám sát tuyển dụng IT (Kafka -> Spark -> MongoDB -> Streamlit)")

if st.button('🔄 Làm mới dữ liệu'):
    st.rerun()

df = get_data()

if not df.empty:
    # ---------------- PHẦN 1: KPI CŨ (GIỮ NGUYÊN) ----------------
    col1, col2, col3 = st.columns(3)
    col1.metric("Tổng tin tuyển dụng", len(df))
    
    if 'currency' in df.columns and 'max_salary' in df.columns:
        df_vnd = df[df['currency'] == 'VND']
        max_sal = df_vnd['max_salary'].max() if not df_vnd.empty else 0
        col2.metric("Lương cao nhất (VND)", f"{max_sal:,.0f} đ" if max_sal else "N/A")
    else:
        col2.metric("Lương cao nhất", "N/A")
        
    latest_comp = df.iloc[0].get('company', df.iloc[0].get('Tên công ty', 'N/A'))
    col3.metric("Công ty mới nhất", latest_comp)

    st.divider()

    # ---------------- PHẦN 2: SKILL MỚI (THÊM VÀO ĐÂY) ----------------
    st.subheader("🛠️ Top Kỹ năng & Công nghệ đang Hot")
    df_skills = extract_skills(df)
    
    if not df_skills.empty:
        # Vẽ biểu đồ Skill to đẹp
        fig_skill = px.bar(df_skills.head(15), x='Skill', y='Count', text='Count', 
                           color='Count', color_continuous_scale='Turbo',
                           labels={'Count': 'Số lượng tin', 'Skill': 'Kỹ năng'})
        st.plotly_chart(fig_skill, use_container_width=True)
    else:
        st.info("Đang chờ dữ liệu mô tả công việc để phân tích kỹ năng...")

    st.divider()

    # ---------------- PHẦN 3: BIỂU ĐỒ CŨ (GIỮ NGUYÊN) ----------------
    # Lọc dữ liệu lương để vẽ biểu đồ
    df_salary = pd.DataFrame()
    if 'max_salary' in df.columns and 'currency' in df.columns:
        df_salary = df[(df['max_salary'] > 0) & (df['currency'] == 'VND')]

    col_chart1, col_chart2 = st.columns(2)
    
    # Biểu đồ Địa điểm (Cũ)
    with col_chart1:
        st.subheader("📍 Phân bố Địa điểm")
        loc_col = 'location' if 'location' in df.columns else 'Địa điểm'
        if loc_col in df.columns:
            loc_counts = df[loc_col].value_counts().reset_index()
            loc_counts.columns = ['location', 'count']
            fig_loc = px.pie(loc_counts, values='count', names='location', hole=0.4)
            st.plotly_chart(fig_loc, use_container_width=True)

    # Biểu đồ Lương (Cũ)
    with col_chart2:
        st.subheader("💰 Phân bố Lương (VND)")
        if not df_salary.empty:
            fig_sal = px.histogram(df_salary, x="max_salary", nbins=20, 
                                   labels={'max_salary': 'Mức lương'}, color_discrete_sequence=['#3366cc'])
            st.plotly_chart(fig_sal, use_container_width=True)
        else:
            st.info("Chưa có đủ dữ liệu lương VND.")

    # ---------------- PHẦN 4: TOP CÔNG TY CŨ (GIỮ NGUYÊN) ----------------
    st.subheader("🏆 Top Công ty tuyển dụng")
    comp_col = 'company' if 'company' in df.columns else 'Tên công ty'
    if comp_col in df.columns:
        top_comp = df[comp_col].value_counts().head(10).reset_index()
        top_comp.columns = ['company', 'count']
        fig_bar = px.bar(top_comp, x='count', y='company', orientation='h', 
                         text='count', color='count', color_continuous_scale='Viridis')
        fig_bar.update_layout(yaxis={'categoryorder':'total ascending'})
        st.plotly_chart(fig_bar, use_container_width=True)

    # ---------------- PHẦN 5: BẢNG DỮ LIỆU CŨ (GIỮ NGUYÊN) ----------------
    with st.expander("📋 Xem dữ liệu chi tiết"):
        st.dataframe(df.head(20), use_container_width=True)

else:
    st.warning("⚠️ Chưa có dữ liệu trong MongoDB. Hãy chạy Spark Job!")

time.sleep(10)
st.rerun()