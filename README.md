# 🛍️ Enterprise Sentiment Analysis Data Warehouse

## 📌 About The Project

هذا المشروع عبارة عن نظام هندسة بيانات متكامل (Data Engineering System) يهدف إلى:

- استخراج تقييمات العملاء.
- تنظيف ومعالجة البيانات.
- تحليل المشاعر باستخدام تقنيات الذكاء الاصطناعي (Machine Learning).
- تخزين النتائج داخل مستودع بيانات مؤسسي (Enterprise Data Warehouse).
- تصميم قاعدة البيانات باستخدام **Star Schema**.
- عرض البيانات والتحليلات من خلال لوحة تحكم تفاعلية باستخدام Streamlit.

---

# 🎬 Project Demo

> 💡 شاهد طريقة عمل النظام ولوحة التحكم التفاعلية:

```
<video src="PASTE_YOUR_COPIED_LINK_HERE" controls="controls" style="max-width: 100%;">
  Your browser does not support the video tag. You can <a href="[PASTE_YOUR_COPIED_LINK_HERE](https://github.com/EgyToxic/Sentiment-reviews-project/raw/refs/heads/main/project%20demo.mp4?download=)">download the video here</a>.
</video>
```

> ملاحظة: قم برفع الفيديو داخل GitHub عن طريق Drag & Drop ثم استبدل الرابط السابق بالرابط الذي سيتم إنشاؤه.

---

# 🛠️ Tech Stack

## Database
- Oracle Database 11g Express Edition (XE)

## ETL Pipeline
- Python
- Pandas
- oracledb

## Artificial Intelligence
- Scikit-Learn
- TF-IDF
- Logistic Regression

## User Interface
- Streamlit

---

# ⚙️ Prerequisites

قبل تشغيل المشروع يجب توفر:

1. Python 3.9+
2. Oracle Database 11g XE
3. Oracle Instant Client 19c (x64)

يستخدم Oracle Instant Client لتفعيل Thick Mode والاتصال مع Oracle Database 11g.

---

# 🚀 Complete Setup Guide

# 1️⃣ Database Setup

## الدخول إلى Oracle كمدير:

```bash
sqlplus / as sysdba
```

---

## إنشاء مستخدم المشروع:

```sql
CREATE USER REVIEWS_DWH IDENTIFIED BY admin123;

GRANT ALL PRIVILEGES TO REVIEWS_DWH;
```

---

## بيانات تسجيل الدخول:

```
Username:
REVIEWS_DWH

Password:
admin123
```

---

## إنشاء جداول قاعدة البيانات:

قم بتنفيذ الملف:

```
database_setup.sql
```

سيتم إنشاء تصميم الـ Star Schema تلقائياً.

---

# 2️⃣ Python Environment Setup

افتح Terminal داخل مجلد المشروع ثم قم بتنفيذ:

```bash
pip install -r requirements.txt
```

---

# 3️⃣ Oracle Instant Client Configuration

افتح الملفات:

```
etl_pipeline.py
app.py
```

وابحث عن:

```python
instant_client_dir
```

ثم قم بتعديل المسار إلى مكان تثبيت:

```
Oracle Instant Client 19c
```

على جهازك.

---

# ▶️ Running The System

## 1. تشغيل ETL Pipeline

نفذ:

```bash
python etl_pipeline.py
```

بعد نجاح التشغيل ستظهر الرسالة:

```
✅ مبروك! الـ ETL Pipeline اكتمل
```

---

## 2. تشغيل Dashboard

نفذ:

```bash
streamlit run app.py
```

سيتم فتح لوحة التحكم تلقائياً في المتصفح.

---

# 🏗️ Data Warehouse Architecture

تم تصميم مستودع البيانات باستخدام:

# ⭐ Star Schema


```
                 Dim_Product
                      |
                      |
                      |
Dim_Date -------- Fact_Reviews
                      |
                      |
              Sentiment Analysis
```


---

# 📂 Database Tables

## Fact_Reviews

الجدول الأساسي ويحتوي على:

- Customer Reviews
- Ratings
- Sentiment Results
- Product References
- Date References


---

## Dim_Product

يحتوي على:

- Product ID
- Product Name
- Product Category


---

## Dim_Date

يحتوي على:

- Date ID
- Day
- Month
- Year


---

# 🤖 Machine Learning Pipeline


```
Customer Reviews

        |
        v

Data Cleaning

        |
        v

TF-IDF Vectorization

        |
        v

Logistic Regression Model

        |
        v

Sentiment Prediction

        |
        v

Oracle Data Warehouse
```

---

# 📁 Project Structure


```
Enterprise-Sentiment-Analysis/

│
├── app.py
│
├── etl_pipeline.py
│
├── database_setup.sql
│
├── requirements.txt
│
├── README.md
│
└── data/
    │
    └── reviews.csv

```

---

# ✅ Features

✔ Automated ETL Pipeline  
✔ Data Cleaning & Transformation  
✔ Machine Learning Sentiment Classification  
✔ Oracle Enterprise Data Warehouse  
✔ Star Schema Design  
✔ Interactive Streamlit Dashboard  
✔ Product & Date Dimensions  
✔ Sentiment Analytics  

---

# 📊 System Workflow


```
Data Source

     |
     v

Python ETL Pipeline

     |
     v

Machine Learning Model

     |
     v

Oracle Data Warehouse

     |
     v

Streamlit Dashboard

```

---
