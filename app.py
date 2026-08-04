import streamlit as st
import pandas as pd
import oracledb
import joblib
from datetime import datetime
import plotly.express as px
import os
import hashlib
import findspark
findspark.init()
import json
import glob
import requests
from bs4 import BeautifulSoup
import time 

# استدعاء مكتبات معالجة البيانات
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, udf
from pyspark.sql.types import StringType

# ==========================================
# 1. إعدادات الصفحة
# ==========================================
st.set_page_config(page_title="Enterprise Review Analyzer", layout="wide")

hide_and_style = """
<style>
#MainMenu {visibility: hidden;} footer {visibility: hidden;} 
div.stButton > button:first-child {
    background-color: #4CAF50; color: white; font-size: 16px;
    border-radius: 8px; border: none; transition: all 0.3s ease;
}
div.stButton > button:first-child:hover { background-color: #45a049; transform: translateY(-2px); }
</style>
"""
st.markdown(hide_and_style, unsafe_allow_html=True)

# ==========================================
# 2. تهيئة بيئة PySpark و Oracle
# ==========================================
@st.cache_resource
def init_spark():
    # تهيئة جلسة سبارك
    return SparkSession.builder \
        .appName("Streamlit_PySpark_Pipeline") \
        .getOrCreate()

if "oracle_initialized" not in st.session_state:
    try:
        instant_client_dir = r"E:\Games\data engnireeing\instantclient-basiclite-windows.x64-19.31.0.0.0dbru\instantclient_19_31"
        oracledb.init_oracle_client(lib_dir=instant_client_dir)
        st.session_state["oracle_initialized"] = True
    except Exception:
        st.session_state["oracle_initialized"] = True

@st.cache_resource
def init_connection():
    return oracledb.connect(user="REVIEWS_DWH", password="admin123", dsn="localhost:1521/XE")

# ==========================================
# 3. تحميل نماذج التعلم الآلي وإعداد محرك الاستنتاجات
# ==========================================
@st.cache_resource
def load_models():
    m = joblib.load('sentiment_model_best.pkl')
    v = joblib.load('tfidf_vectorizer_best.pkl')
    return m, v

model, vectorizer = load_models()

# دالة تحليل المشاعر الأساسية
def get_sentiment(text):
    if text is None or str(text).strip() == "":
        return 'Neutral'
    vec_text = vectorizer.transform([str(text)])
    pred = model.predict(vec_text)[0]
    return 'Positive' if pred == 1 else 'Negative'

# دالة استخراج سبب المشكلة بناءً على الكلمات المفتاحية
def get_reason(text, sentiment):
    if text is None:
        return "No Review Provided"
    text_lower = str(text).lower()
    if sentiment == "Negative":
        if any(w in text_lower for w in ["battery", "charger", "power", "cord", "plug"]): return "Hardware and Power Issues"
        if any(w in text_lower for w in ["delivery", "shipping", "shipped", "days", "took", "merchant"]): return "Logistics and Shipping Delay"
        if any(w in text_lower for w in ["fit", "size", "tight", "small", "large", "fabric"]): return "Product Sizing and Fit Defect"
        if any(w in text_lower for w in ["broken", "peeling", "cheap", "flimsy", "trash", "junk"]): return "Product Quality and Build Material"
        if any(w in text_lower for w in ["menu", "scroll", "buttons", "software", "crash"]): return "Software Interface Bug"
        return "General Product Dissatisfaction"
    else:
        if any(w in text_lower for w in ["love", "great", "excellent", "wonderful", "amazing"]): return "High Customer Satisfaction"
        return "Positive User Feedback"

# دالة توليد التوصيات والحلول لإدارة الأعمال
def get_recommendation(reason):
    mapping = {
        "Hardware and Power Issues": "Enhance hardware battery capacity and optimize power firmware.",
        "Logistics and Shipping Delay": "Optimize last-mile logistics networks and review SLAs.",
        "Product Sizing and Fit Defect": "Update digital size charts and refine manufacturing dimensions.",
        "Product Quality and Build Material": "Conduct rigorous vendor quality control audits.",
        "Software Interface Bug": "Prioritize user interface hotfixes in the next software release.",
        "General Product Dissatisfaction": "Initiate direct customer outreach to gather feedback.",
        "High Customer Satisfaction": "Leverage for testimonial marketing and loyalty programs.",
        "Positive User Feedback": "Incorporate user highlights into marketing collateral."
    }
    return mapping.get(reason, "Monitor customer feedback trends.")

# تغليف الدوال السابقة لتعمل بشكل موزع داخل PySpark
predict_sentiment_udf = udf(get_sentiment, StringType())
extract_reason_udf = udf(get_reason, StringType())
get_recommendation_udf = udf(get_recommendation, StringType())

# دالة مساعدة لرفع البيانات لقاعدة أوراكل مع توليد البيانات الناقصة
def insert_record_to_dwh(cursor, text, sentiment, reason, recommendation, source):
    text_hash = int(hashlib.md5(str(text).encode('utf-8')).hexdigest(), 16)
    
    age = 20 + (text_hash % 45)
    gender = "Female" if "Womens" in source else ("Male" if (text_hash % 2 == 0) else "Female")
    customer_name = f"Customer_{gender[0]}_{text_hash % 1000}"
    
    category = "Apparel" if "Womens" in source else "General Electronics"
    brands = ["LogiTech", "Sony", "Anker", "Apple", "Samsung", "Wrangler"]
    brand = brands[text_hash % len(brands)]
    product_name = f"Product_{brand}_{text_hash % 50}"
    
    countries = ["United States", "United Kingdom", "Egypt", "Canada", "Germany"]
    cities = {
        "United States": ["New York", "Los Angeles", "Chicago"],
        "United Kingdom": ["London", "Manchester", "Birmingham"],
        "Egypt": ["Cairo", "Alexandria", "Giza"],
        "Canada": ["Toronto", "Vancouver", "Montreal"],
        "Germany": ["Berlin", "Munich", "Frankfurt"]
    }
    country = countries[text_hash % len(countries)]
    city = cities[country][text_hash % len(cities[country])]
    
    cursor.execute("SELECT Product_ID FROM Dim_Product WHERE Product_Name = :1", [product_name])
    res = cursor.fetchone()
    if res: product_id = res[0]
    else:
        v = cursor.var(int)
        cursor.execute("INSERT INTO Dim_Product (Product_Name, Category, Brand) VALUES (:1, :2, :3) RETURNING Product_ID INTO :4", [product_name, category, brand, v])
        product_id = v.getvalue()[0]
        
    cursor.execute("SELECT Customer_ID FROM Dim_Customer WHERE Customer_Name = :1", [customer_name])
    res = cursor.fetchone()
    if res: customer_id = res[0]
    else:
        v = cursor.var(int)
        cursor.execute("INSERT INTO Dim_Customer (Customer_Name, Age, Gender) VALUES (:1, :2, :3) RETURNING Customer_ID INTO :4", [customer_name, age, gender, v])
        customer_id = v.getvalue()[0]
        
    cursor.execute("SELECT Location_ID FROM Dim_Location WHERE Country = :1 AND City = :2", [country, city])
    res = cursor.fetchone()
    if res: location_id = res[0]
    else:
        v = cursor.var(int)
        cursor.execute("INSERT INTO Dim_Location (Country, City) VALUES (:1, :2) RETURNING Location_ID INTO :3", [country, city, v])
        location_id = v.getvalue()[0]
        
    now = datetime.now()
    cursor.execute("SELECT Date_ID FROM Dim_Date WHERE Review_Year = :1 AND Review_Month = :2 AND Review_Day = :3", [now.year, now.month, now.day])
    res = cursor.fetchone()
    if res: date_id = res[0]
    else:
        v = cursor.var(int)
        cursor.execute("INSERT INTO Dim_Date (Review_Date, Review_Year, Review_Month, Review_Day) VALUES (:1, :2, :3, :4) RETURNING Date_ID INTO :5", [now, now.year, now.month, now.day, v])
        date_id = v.getvalue()[0]
        
    cursor.execute("""
        INSERT INTO Fact_Reviews 
        (Product_ID, Customer_ID, Date_ID, Location_ID, Review_Text, Sentiment_Label, Issue_Reason, AI_Recommendation, Source_Link)
        VALUES (:1, :2, :3, :4, :5, :6, :7, :8, :9)
    """, [product_id, customer_id, date_id, location_id, str(text), sentiment, reason, recommendation, source])

# ==========================================
# 4. القائمة الجانبية لمعالجة البيانات
# ==========================================
with st.sidebar:
    st.header("Data Pipelines (PySpark)")
    st.markdown("Distributed Processing for bulk datasets.")
    
    tab_local, tab_kaggle, tab_scrape = st.tabs(["Local CSV", "Kaggle API", "Web Scraping"])
    
    # التبويب الأول: معالجة ملفات CSV المحلية
    with tab_local:
        local_csv = st.file_uploader("Upload your CSV file", type=['csv'])
        if st.button("Process Local CSV"):
            if local_csv:
                with st.status("Processing PySpark pipeline...", expanded=True) as status:
                    try:
                        st.write("Starting Spark Session...")
                        spark = init_spark()
                        
                        temp_csv_path = "temp_upload.csv"
                        with open(temp_csv_path, "wb") as f:
                            f.write(local_csv.getbuffer())
                            
                        st.write("Reading CSV via Spark engine and detecting columns...")
                        sdf = spark.read.csv(temp_csv_path, header=True, inferSchema=True).limit(100)
                        
                        possible_names = ['review', 'text', 'comment', 'feedback', 'message', 'content']
                        review_col = next((c for c in sdf.columns if any(k in c.lower() for k in possible_names)), None)
                        if not review_col:
                            for field in sdf.schema.fields:
                                if isinstance(field.dataType, StringType):
                                    review_col = field.name; break
                        
                        if review_col:
                            st.write("Running Distributed AI Model & Reasoning Engine...")
                            sdf = sdf.withColumn("Sentiment", predict_sentiment_udf(col(review_col)))
                            sdf = sdf.withColumn("Reason", extract_reason_udf(col(review_col), col("Sentiment")))
                            sdf = sdf.withColumn("Recommendation", get_recommendation_udf(col("Reason")))
                            
                            st.write("Uploading enriched data to Oracle...")
                            collected_data = sdf.select(review_col, "Sentiment", "Reason", "Recommendation").collect()
                            
                            conn = init_connection()
                            cursor = conn.cursor()
                            
                            progress_bar = st.progress(0)
                            metrics_text = st.empty()
                            total_rows = len(collected_data)
                            inserted = 0
                            start_time = time.time()
                            
                            source_name = f"Local CSV ({local_csv.name})"
                            
                            for i, row in enumerate(collected_data):
                                insert_record_to_dwh(cursor, row[review_col], row["Sentiment"], row["Reason"], row["Recommendation"], source_name)
                                inserted += 1
                                
                                progress_ratio = inserted / total_rows
                                progress_bar.progress(progress_ratio)
                                elapsed_time = time.time() - start_time
                                eta_seconds = (elapsed_time / inserted) * (total_rows - inserted) if inserted > 0 else 0
                                metrics_text.caption(f"Progress: {int(progress_ratio*100)}% | ETA: {time.strftime('%M:%S', time.gmtime(eta_seconds))} | Processed: {inserted}/{total_rows}")
                                
                            conn.commit()
                            cursor.close()
                            if os.path.exists(temp_csv_path): os.remove(temp_csv_path)
                            
                            status.update(label=f"PySpark completed. {inserted} reviews saved.", state="complete", expanded=False)
                        else:
                            status.update(label="Failed to detect text column.", state="error")
                    except Exception as e:
                        status.update(label=f"Error: {e}", state="error")
            else:
                st.warning("Please upload a CSV file first.")

    # التبويب الثاني: جلب البيانات من Kaggle
    with tab_kaggle:
        kaggle_file = st.file_uploader("Upload kaggle.json Key", type=['json'])
        dataset_slug = st.text_input("Dataset Slug", placeholder="e.g. zaynawad/amazon-reviews")
        if st.button("Fetch Kaggle Dataset"):
            if kaggle_file and dataset_slug:
                with st.status("Connecting to Kaggle API...", expanded=True) as status:
                    try:
                        creds = json.load(kaggle_file)
                        os.environ['KAGGLE_USERNAME'] = creds['username']
                        os.environ['KAGGLE_KEY'] = creds['key']
                        
                        from kaggle.api.kaggle_api_extended import KaggleApi
                        api = KaggleApi()
                        api.authenticate()
                        
                        download_dir = "./kaggle_temp"
                        os.makedirs(download_dir, exist_ok=True)
                        api.dataset_download_files(dataset_slug, path=download_dir, unzip=True)
                        
                        csv_files = glob.glob(f"{download_dir}/*.csv")
                        if not csv_files:
                            status.update(label="No CSV file found in dataset.", state="error")
                        else:
                            spark = init_spark()
                            sdf = spark.read.csv(csv_files[0], header=True, inferSchema=True).limit(100)
                            
                            possible_names = ['review', 'text', 'comment', 'feedback', 'message', 'content']
                            review_col = next((c for c in sdf.columns if any(k in c.lower() for k in possible_names)), None)
                            if not review_col:
                                for field in sdf.schema.fields:
                                    if isinstance(field.dataType, StringType): review_col = field.name; break
                            
                            if review_col:
                                sdf = sdf.withColumn("Sentiment", predict_sentiment_udf(col(review_col)))
                                sdf = sdf.withColumn("Reason", extract_reason_udf(col(review_col), col("Sentiment")))
                                sdf = sdf.withColumn("Recommendation", get_recommendation_udf(col("Reason")))
                                
                                collected_data = sdf.select(review_col, "Sentiment", "Reason", "Recommendation").collect()
                                
                                conn = init_connection()
                                cursor = conn.cursor()
                                kaggle_link = f"https://www.kaggle.com/datasets/{dataset_slug}"
                                
                                progress_bar = st.progress(0)
                                metrics_text = st.empty()
                                total_rows = len(collected_data)
                                inserted = 0
                                start_time = time.time()
                                
                                for i, row in enumerate(collected_data):
                                    insert_record_to_dwh(cursor, row[review_col], row["Sentiment"], row["Reason"], row["Recommendation"], kaggle_link)
                                    inserted += 1
                                    
                                    progress_ratio = inserted / total_rows
                                    progress_bar.progress(progress_ratio)
                                    elapsed_time = time.time() - start_time
                                    eta_seconds = (elapsed_time / inserted) * (total_rows - inserted) if inserted > 0 else 0
                                    metrics_text.caption(f"Progress: {int(progress_ratio*100)}% | ETA: {time.strftime('%M:%S', time.gmtime(eta_seconds))} | Processed: {inserted}/{total_rows}")
                                    
                                conn.commit()
                                cursor.close()
                                status.update(label=f"Pipeline completed. {inserted} reviews saved.", state="complete", expanded=False)
                            else:
                                status.update(label="Auto-detection failed.", state="error")
                    except Exception as e:
                        status.update(label=f"Error: {e}", state="error")
            else:
                st.warning("Please provide Kaggle credentials and a dataset slug.")

    # التبويب الثالث: استخراج البيانات من الويب
    with tab_scrape:
        target_url = st.text_input("Enter Product Reviews URL:", placeholder="https://www.amazon.com/...")
        if st.button("Start Web Scraping"):
            if target_url:
                with st.status("Initiating Web Scraper...", expanded=True) as status:
                    try:
                        headers = {'User-Agent': 'Mozilla/5.0'}
                        response = requests.get(target_url, headers=headers, timeout=10)
                        
                        soup = BeautifulSoup(response.text, 'html.parser')
                        reviews_elements = soup.find_all('span', {'data-hook': 'review-body'})
                        if not reviews_elements:
                            reviews_elements = [p for p in soup.find_all('p') if len(p.text) > 40]

                        scraped_texts = [el.text.strip() for el in reviews_elements if el.text.strip()]

                        if scraped_texts:
                            scraped_texts = scraped_texts[:50]
                            
                            conn = init_connection()
                            cursor = conn.cursor()
                            
                            progress_bar = st.progress(0)
                            metrics_text = st.empty()
                            total_rows = len(scraped_texts)
                            inserted = 0
                            start_time = time.time()
                            
                            for i, text in enumerate(scraped_texts):
                                sentiment = get_sentiment(text)
                                reason = get_reason(text, sentiment)
                                recommendation = get_recommendation(reason)
                                
                                insert_record_to_dwh(cursor, text, sentiment, reason, recommendation, target_url)
                                inserted += 1
                                
                                progress_ratio = inserted / total_rows
                                progress_bar.progress(progress_ratio)
                                elapsed_time = time.time() - start_time
                                eta_seconds = (elapsed_time / inserted) * (total_rows - inserted) if inserted > 0 else 0
                                metrics_text.caption(f"Progress: {int(progress_ratio*100)}% | ETA: {time.strftime('%M:%S', time.gmtime(eta_seconds))} | Processed: {inserted}/{total_rows}")
                                
                            conn.commit()
                            cursor.close()
                            status.update(label=f"Scraping complete. {inserted} reviews saved.", state="complete", expanded=False)
                        else:
                            status.update(label="No reviews found.", state="error")
                    except Exception as e:
                        status.update(label=f"Error: {e}", state="error")
            else:
                st.warning("Please enter a valid URL.")

    # ==========================================
    # إعدادات قاعدة البيانات للحذف
    # ==========================================
    st.divider()
    with st.expander("Database Admin", expanded=False):
        delete_id = st.number_input("Delete Review by ID:", min_value=1, step=1)
        if st.button("Delete Single Review"):
            try:
                conn = init_connection()
                cursor = conn.cursor()
                cursor.execute("DELETE FROM Fact_Reviews WHERE Review_ID = :1", [delete_id])
                if cursor.rowcount > 0:
                    conn.commit()
                    st.success(f"Review #{delete_id} deleted. Refresh to see changes.")
                else:
                    st.error(f"Review #{delete_id} not found.")
                cursor.close()
            except Exception as e:
                st.error(f"DB Error: {e}")
                
        st.divider()
        if st.button("DELETE ALL REVIEWS"):
            try:
                conn = init_connection()
                cursor = conn.cursor()
                cursor.execute("DELETE FROM Fact_Reviews")
                conn.commit()
                cursor.close()
                st.success("All reviews cleared. Refresh to see changes.")
            except Exception as e:
                st.error(f"DB Error: {e}")

# ==========================================
# 5. واجهة المستخدم الرئيسية والتحليلات
# ==========================================
st.title("Enterprise Sentiment Analysis Dashboard")
st.markdown("Powered by: Advanced Star Schema, PySpark, Oracle DWH & NLP Reasoning")
st.divider()

col1, col2 = st.columns([1.2, 1])

with col1:
    st.subheader("Analyze Manual Review")
    user_review = st.text_area(
        "Enter customer review:", 
        height=130, 
        placeholder="e.g., The delivery took 10 days, this is completely unacceptable!"
    )
    
    if st.button("Analyze & Save to DWH"):
        if user_review.strip() == "":
            st.warning("Please enter a review.")
        else:
            sentiment_result = get_sentiment(user_review)
            reason_result = get_reason(user_review, sentiment_result)
            recommendation_result = get_recommendation(reason_result)
            
            if sentiment_result == "Positive":
                st.success(f"Result: {sentiment_result}")
            else:
                st.error(f"Result: {sentiment_result}")
                st.warning(f"Extracted Issue: {reason_result}")
                st.info(f"AI Recommendation: {recommendation_result}")

            try:
                conn = init_connection()
                cursor = conn.cursor()
                insert_record_to_dwh(cursor, user_review, sentiment_result, reason_result, recommendation_result, 'Manual Entry')
                conn.commit()
                cursor.close()
                st.info("Review processed and structured into Dimensions & Facts.")
            except Exception as e:
                st.error(f"DB Error: {e}")

with col2:
    st.subheader("Business Intelligence Analytics")
    try:
        conn = init_connection()
        query_stats = """
            SELECT 
                f.Sentiment_Label as "Sentiment", 
                c.Gender as "Gender", 
                c.Age as "Age", 
                p.Category as "Category",
                f.Issue_Reason as "Reason"
            FROM Fact_Reviews f
            JOIN Dim_Customer c ON f.Customer_ID = c.Customer_ID
            JOIN Dim_Product p ON f.Product_ID = p.Product_ID
        """
        stats_df = pd.read_sql(query_stats, conn)
        
        if not stats_df.empty:
            stats_df['Age Group'] = pd.cut(stats_df['Age'], bins=[0, 25, 35, 50, 100], labels=['18-25', '26-35', '36-50', '50+'])
            
            # إضافة تبويب Overview للرسم البياني الدائري (Pie Chart)
            tab1, tab2, tab3, tab4 = st.tabs(["Overview", "Demographics", "Categories", "NLP Insights"])
            
            with tab1:
                # تجميع عدد التقييمات حسب المشاعر لرسم הـ Pie Chart
                sentiment_counts = stats_df['Sentiment'].value_counts().reset_index()
                sentiment_counts.columns = ['Sentiment', 'Count']
                
                fig_pie = px.pie(sentiment_counts, names='Sentiment', values='Count', hole=0.4,
                                 color='Sentiment', color_discrete_map={'Positive':'#28a745', 'Negative':'#dc3545'},
                                 height=250)
                fig_pie.update_layout(margin=dict(l=0, r=0, t=30, b=0))
                st.plotly_chart(fig_pie, use_container_width=True)
                
            with tab2:
                fig_gender = px.histogram(stats_df, x='Gender', color='Sentiment', barmode='group', height=250,
                                          color_discrete_map={'Positive':'#28a745', 'Negative':'#dc3545'})
                fig_gender.update_layout(margin=dict(l=0, r=0, t=30, b=0))
                st.plotly_chart(fig_gender, use_container_width=True)
                
            with tab3:
                fig_cat = px.histogram(stats_df, x='Category', color='Sentiment', barmode='group', height=250,
                                          color_discrete_map={'Positive':'#28a745', 'Negative':'#dc3545'})
                fig_cat.update_layout(margin=dict(l=0, r=0, t=30, b=0))
                st.plotly_chart(fig_cat, use_container_width=True)
                
            with tab4:
                fig_reason = px.histogram(stats_df, y='Reason', color='Sentiment', height=250,
                                          color_discrete_map={'Positive':'#28a745', 'Negative':'#dc3545'})
                fig_reason.update_layout(margin=dict(l=0, r=0, t=30, b=0), showlegend=False)
                st.plotly_chart(fig_reason, use_container_width=True)
        else:
            st.info("No data available to plot.")
    except Exception as e:
        st.error(f"Error generating charts: {e}")

# ==========================================
# 6. عرض البيانات من المستودع
# ==========================================
st.divider()
st.subheader("Data Warehouse Records (Live Integration)")
try:
    conn = init_connection()
    query = """
        SELECT * FROM (
            SELECT 
                f.Review_ID AS "ID",
                c.Gender AS "Gender",
                c.Age AS "Age",
                f.Review_Text AS "Review",
                f.Sentiment_Label AS "Sentiment",
                f.Issue_Reason AS "Extracted Reason",
                f.AI_Recommendation AS "AI Action",
                l.Country AS "Country",
                f.Source_Link AS "Source"
            FROM Fact_Reviews f
            JOIN Dim_Customer c ON f.Customer_ID = c.Customer_ID
            JOIN Dim_Location l ON f.Location_ID = l.Location_ID
            ORDER BY f.Review_ID DESC
        ) WHERE ROWNUM <= 100
    """
    df = pd.read_sql(query, conn)
    df['Review'] = df['Review'].astype(str)
    
    # فصل نوع المصدر عن الرابط الفعلي
    def get_source_type(src):
        src = str(src)
        if src.startswith('http'):
            return 'Kaggle Dataset' if 'kaggle' in src.lower() else 'Web Scraped'
        return 'Manual Entry' if 'Manual' in src else 'Local CSV'

    df['Source Type'] = df['Source'].apply(get_source_type)
    df['Source Link'] = df['Source'].apply(lambda x: x if str(x).startswith('http') else None)
    
    # إعادة ترتيب الأعمدة للعرض النهائي
    display_df = df[['ID', 'Gender', 'Age', 'Review', 'Sentiment', 'Extracted Reason', 'AI Action', 'Country', 'Source Type', 'Source Link']]
    
    st.dataframe(
        display_df,
        column_config={
            "Sentiment": st.column_config.TextColumn("Sentiment"),
            "Source Link": st.column_config.LinkColumn("Source Link", display_text="View Link")
        },
        width="stretch",
        hide_index=True
    )
except Exception as e:
    st.error(f"Error loading records: {e}")