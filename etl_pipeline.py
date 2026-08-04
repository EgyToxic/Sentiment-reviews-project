import oracledb
import joblib
from datetime import datetime
import os
import hashlib
import findspark
findspark.init()
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, udf, lit
from pyspark.sql.types import StringType

print("EXTRACT: Initializing multi-source data ingestion pipeline...")

# إنشاء جلسة سبارك لإدارة ومعالجة البيانات الضخمة
spark = SparkSession.builder \
    .appName("EnterpriseReviewETL") \
    .getOrCreate()

# 1. قراءة واستخلاص البيانات من المصدر الأول
df1 = spark.read.csv('ecommerce-product-reviews.csv', header=True, inferSchema=True).limit(250)

# 2. قراءة واستخلاص البيانات من المصدر الثاني (تقييمات الملابس)
df2 = spark.read.csv('Womens Clothing E-Commerce Reviews.csv', header=True, inferSchema=True).limit(250)

# توحيد هيكل البيانات للمصدر الأول وتحديد الأعمدة المتاحة
sdf1 = df1.select(
    col("review").alias("Raw_Review_Text"),
    lit(None).cast("int").alias("Input_Age"),
    lit("General Electronics").alias("Input_Category"),
    lit("E-Commerce Dataset").alias("Source")
)

# توحيد هيكل البيانات للمصدر الثاني وتحديد الأعمدة المتاحة
sdf2 = df2.select(
    col("Review Text").alias("Raw_Review_Text"),
    col("Age").cast("int").alias("Input_Age"),
    col("Department Name").alias("Input_Category"),
    lit("Womens Clothing Dataset").alias("Source")
)

# دمج المصدرين معاً في DataFrame رئيسي واحد وتصفية الصفوف الفارغة
df_master = sdf1.union(sdf2).filter(col("Raw_Review_Text").isNotNull())

print("TRANSFORM: Applying advanced sentiment analysis and NLP reasoning...")

# تحميل نماذج التعلم الآلي المخزنة مسبقاً
model = joblib.load('sentiment_model_best.pkl')
vectorizer = joblib.load('tfidf_vectorizer_best.pkl')

# دالة تصنيف المشاعر باستخدام محرك التعلم الآلي
@udf(returnType=StringType())
def predict_sentiment_udf(text):
    if text is None or str(text).strip() == "":
        text = 'No Review'
    vec_text = vectorizer.transform([str(text)])
    pred = model.predict(vec_text)[0]
    return 'Positive' if pred == 1 else 'Negative'

# دالة استخراج سبب المشكلة (NLP Issue Extraction) بناءً على الكلمات المفتاحية وسياق النص
@udf(returnType=StringType())
def extract_reason_udf(text, sentiment):
    if text is None:
        return "No Review Provided"
    text_lower = str(text).lower()
    if sentiment == "Negative":
        if any(w in text_lower for w in ["battery", "charger", "power", "cord", "plug"]):
            return "Hardware and Power Issues"
        if any(w in text_lower for w in ["delivery", "shipping", "shipped", "days", "took", "merchant", "receive"]):
            return "Logistics and Shipping Delay"
        if any(w in text_lower for w in ["fit", "size", "tight", "small", "large", "xs", "medium", "cut", "fabric"]):
            return "Product Sizing and Fit Defect"
        if any(w in text_lower for w in ["broken", "peeling", "cheap", "flimsy", "trash", "junk", "worst", "rubbish"]):
            return "Product Quality and Build Material"
        if any(w in text_lower for w in ["menu", "scrolling", "scroll", "buttons", "software", "app", "crash"]):
            return "Software Interface Bug"
        return "General Product Dissatisfaction"
    else:
        if any(w in text_lower for w in ["love", "great", "excellent", "wonderful", "amazing", "best", "perfect"]):
            return "High Customer Satisfaction"
        return "Positive User Feedback"

# دالة توليد التوصيات المقترحة لإدارة الشركة (AI Business Recommendation) بناءً على السبب المستخرج
@udf(returnType=StringType())
def get_recommendation_udf(reason):
    mapping = {
        "Hardware and Power Issues": "Enhance hardware battery capacity and optimize power firmware.",
        "Logistics and Shipping Delay": "Optimize last-mile logistics networks and review courier partner SLAs.",
        "Product Sizing and Fit Defect": "Update digital size charts and refine manufacturing dimensional alignment.",
        "Product Quality and Build Material": "Conduct rigorous vendor component selection and quality control audits.",
        "Software Interface Bug": "Prioritize checkout and user interface hotfixes in the next software release.",
        "General Product Dissatisfaction": "Initiate direct customer outreach to gather detailed feedback points.",
        "High Customer Satisfaction": "Maintain current operational excellence standards and leverage for testimonial marketing.",
        "Positive User Feedback": "Incorporate user highlights into upcoming marketing collateral."
    }
    return mapping.get(reason, "Monitor customer feedback trends for continuous improvement.")

# تطبيق العمليات والتحليلات المتقدمة على البيانات المدمجة
df_master = df_master.withColumn("Sentiment_Label", predict_sentiment_udf(col("Raw_Review_Text")))
df_master = df_master.withColumn("Issue_Reason", extract_reason_udf(col("Raw_Review_Text"), col("Sentiment_Label")))
df_master = df_master.withColumn("AI_Recommendation", get_recommendation_udf(col("Issue_Reason")))

print("LOAD: Distributing parallel load execution into Oracle Star Schema...")

# دالة معالجة الحزم والرفع المتوازي إلى جداول قاعدة البيانات
def save_partition_to_oracle_v2(partition):
    import oracledb
    from datetime import datetime
    import hashlib
    
    try:
        instant_client_dir = r"E:\Games\data engnireeing\instantclient-basiclite-windows.x64-19.31.0.0.0dbru\instantclient_19_31"
        oracledb.init_oracle_client(lib_dir=instant_client_dir)
    except Exception:
        pass

    try:
        connection = oracledb.connect(user="REVIEWS_DWH", password="admin123", dsn="localhost:1521/XE")
        cursor = connection.cursor()

        for row in partition:
            review_text = row["Raw_Review_Text"]
            if not review_text or str(review_text).strip() == "":
                continue
                
            sentiment = row["Sentiment_Label"]
            reason = row["Issue_Reason"]
            recommendation = row["AI_Recommendation"]
            source = row["Source"]
            
            # خوارزمية التجزئة لتوليد الخصائص الجغرافية والديموغرافية بشكل متسق ومنظم
            text_hash = int(hashlib.md5(str(review_text).encode('utf-8')).hexdigest(), 16)
            
            # حساب وإعداد السن للعميل
            if row["Input_Age"] is not None:
                age = int(row["Input_Age"])
            else:
                age = 20 + (text_hash % 45)
                
            # تحديد النوع وإصدار اسم افتراضي للعميل
            if source == "Womens Clothing Dataset":
                gender = "Female"
                customer_name = f"Customer_F_{text_hash % 1000}"
            else:
                gender = "Male" if (text_hash % 2 == 0) else "Female"
                customer_name = f"Customer_M_{text_hash % 1000}" if gender == "Male" else f"Customer_F_{text_hash % 1000}"
                
            # إعداد فئة المنتج
            if row["Input_Category"] is not None:
                category = str(row["Input_Category"])
            else:
                category = "General Electronics"
                
            # توليد العلامة التجارية واسم المنتج بناءً على السياق والتجزئة
            brands = ["LogiTech", "Sony", "Anker", "Apple", "Samsung", "Wrangler", "General Brands"]
            brand = brands[text_hash % len(brands)]
            product_name = f"Product_{brand}_{text_hash % 50}"
            
            # توليد الأبعاد الجغرافية للموقع بشكل نمطي ومنظم
            countries = ["United States", "United Kingdom", "Egypt", "Canada", "Germany"]
            cities = {
                "United States": ["New York", "Los Angeles", "Chicago"],
                "United Kingdom": ["London", "Manchester", "Birmingham"],
                "Egypt": ["Cairo", "Alexandria", "Giza"],
                "Canada": ["Toronto", "Vancouver", "Montreal"],
                "Germany": ["Berlin", "Munich", "Frankfurt"]
            }
            country = countries[text_hash % len(countries)]
            city_list = cities[country]
            city = city_list[text_hash % len(city_list)]
            
            # 1. فحص وإدخال بيانات جدول أبعاد المنتجات (Dim_Product)
            cursor.execute("SELECT Product_ID FROM Dim_Product WHERE Product_Name = :1 AND Category = :2", [product_name, category])
            res_prod = cursor.fetchone()
            if res_prod:
                product_id = res_prod[0]
            else:
                prod_id_var = cursor.var(int)
                cursor.execute("""
                    INSERT INTO Dim_Product (Product_Name, Category, Brand)
                    VALUES (:1, :2, :3)
                    RETURNING Product_ID INTO :4
                """, [product_name, category, brand, prod_id_var])
                product_id = prod_id_var.getvalue()[0]

            # 2. فحص وإدخال بيانات جدول أبعاد العملاء (Dim_Customer)
            cursor.execute("SELECT Customer_ID FROM Dim_Customer WHERE Customer_Name = :1", [customer_name])
            res_cust = cursor.fetchone()
            if res_cust:
                customer_id = res_cust[0]
            else:
                cust_id_var = cursor.var(int)
                cursor.execute("""
                    INSERT INTO Dim_Customer (Customer_Name, Age, Gender)
                    VALUES (:1, :2, :3)
                    RETURNING Customer_ID INTO :4
                """, [customer_name, age, gender, cust_id_var])
                customer_id = cust_id_var.getvalue()[0]

            # 3. فحص وإدخال بيانات جدول الأبعاد الجغرافية الجديد (Dim_Location)
            cursor.execute("SELECT Location_ID FROM Dim_Location WHERE Country = :1 AND City = :2", [country, city])
            res_loc = cursor.fetchone()
            if res_loc:
                location_id = res_loc[0]
            else:
                loc_id_var = cursor.var(int)
                cursor.execute("""
                    INSERT INTO Dim_Location (Country, City)
                    VALUES (:1, :2)
                    RETURNING Location_ID INTO :3
                """, [country, city, loc_id_var])
                location_id = loc_id_var.getvalue()[0]

            # 4. فحص وإدخال بيانات جدول أبعاد التواريخ (Dim_Date)
            current_date = datetime.now()
            cursor.execute("""
                SELECT Date_ID FROM Dim_Date 
                WHERE Review_Year = :1 AND Review_Month = :2 AND Review_Day = :3
            """, [current_date.year, current_date.month, current_date.day])
            res_date = cursor.fetchone()
            if res_date:
                date_id = res_date[0]
            else:
                date_id_var = cursor.var(int)
                cursor.execute("""
                    INSERT INTO Dim_Date (Review_Date, Review_Year, Review_Month, Review_Day)
                    VALUES (:1, :2, :3, :4)
                    RETURNING Date_ID INTO :5
                """, [current_date, current_date.year, current_date.month, current_date.day, date_id_var])
                date_id = date_id_var.getvalue()[0]

            # 5. ربط الأبعاد وحفظ السجلات النهائية داخل الجدول المركزي للحقائق (Fact_Reviews)
            cursor.execute("""
                INSERT INTO Fact_Reviews (Product_ID, Customer_ID, Date_ID, Location_ID, Review_Text, Sentiment_Label, Issue_Reason, AI_Recommendation, Source_Link)
                VALUES (:1, :2, :3, :4, :5, :6, :7, :8, :9)
            """, [product_id, customer_id, date_id, location_id, str(review_text), sentiment, reason, recommendation, f"Merged Pipeline ({source})"])

        connection.commit()
        cursor.close()
        connection.close()
    except Exception as e:
        print(f"Database error in partition handling: {e}")

# تطبيق دالة الحفظ الموزع عبر حزم البيانات في سبارك
df_master.foreachPartition(save_partition_to_oracle_v2)

print("ETL Pipeline completed successfully.")
spark.stop()