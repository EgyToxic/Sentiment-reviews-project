# 🛍️ Enterprise Sentiment Analysis Data Warehouse

## 📌 عن المشروع
هذا المشروع عبارة عن نظام هندسة بيانات متكامل (Data Engineering System) يهدف إلى استخراج تقييمات العملاء، تحليلها باستخدام الذكاء الاصطناعي (Machine Learning)، وتخزينها في مستودع بيانات مؤسسي (Enterprise Data Warehouse) مصمم بهيكل **Star Schema**. كما يحتوي على لوحة تحكم تفاعلية (Dashboard) لعرض الإحصائيات وإدخال بيانات جديدة.

## 🛠️ التقنيات المستخدمة (Tech Stack)
- **قاعدة البيانات:** Oracle Database 11g Express Edition (XE)
- **برمجة مسار البيانات (ETL):** Python (Pandas, oracledb)
- **الذكاء الاصطناعي (AI):** Scikit-Learn (Logistic Regression & TF-IDF)
- **واجهة المستخدم (UI):** Streamlit

---

## ⚙️ متطلبات التشغيل الأساسية (Prerequisites)
قبل تشغيل المشروع، تأكد من توافر الآتي على جهازك:
1. تثبيت **Python 3.9+**
2. تثبيت **Oracle Database 11g**
3. تحميل ملفات **Oracle Instant Client 19c** (نسخة x64) وفك ضغطها في مسار معروف لتفعيل وضع الـ Thick Mode (الضروري للاتصال بـ Oracle 11g)

---

## 🚀 خطوات التشغيل الكاملة (Complete Setup Guide)

### الخطوة الأولى: إعداد قاعدة البيانات (Database Setup)

1. قم بفتح موجه الأوامر (CMD) وادخل إلى قاعدة البيانات بصلاحيات المدير:
```bash
sqlplus / as sysdba
```

2. إنشاء المستخدم الخاص بالمشروع ومنحه الصلاحيات:
```sql
CREATE USER REVIEWS_DWH IDENTIFIED BY admin123;
GRANT ALL PRIVILEGES TO REVIEWS_DWH;
```

3. تسجيل الدخول بالمستخدم الجديد:
- **Username:** `REVIEWS_DWH`
- **Password:** `admin123`

4. تشغيل ملف إنشاء الجداول:
قم بتنفيذ ملف `database_setup.sql` لإنشاء Star Schema تلقائياً.

---

### الخطوة الثانية: إعداد بيئة بايثون (Python Environment)

1. افتح التيرمنال داخل مجلد المشروع.
2. ثبّت المكتبات المطلوبة:
```bash
pip install -r requirements.txt
```

---

### الخطوة الثالثة: ضبط مسار Oracle Instant Client

- افتح ملف `etl_pipeline.py` وملف `app.py`.
- ابحث عن المتغير:
  ```python
  instant_client_dir
  ```
- عدّل المسار ليطابق مكان تثبيت **Oracle Instant Client 19c** على جهازك.

---

## ▶️ تشغيل النظام (Running the System)

### 1. تشغيل ETL Pipeline
```bash
python etl_pipeline.py
```

انتظر حتى تظهر رسالة:
> ✅ مبروك! الـ ETL Pipeline اكتمل

---

### 2. تشغيل لوحة التحكم (Streamlit Dashboard)
```bash
streamlit run app.py
```

سيتم فتح لوحة التحكم تلقائياً في المتصفح.

---

## 📂 هيكل قاعدة البيانات (Data Architecture)

تم تصميم مستودع البيانات بنظام **Star Schema** ويتكون من:

- **Fact_Reviews**: الجدول الرئيسي الذي يحتوي على التقييمات ونتائج تحليل المشاعر.
- **Dim_Product**: بيانات المنتجات وتصنيفاتها.
- **Dim_Date**: الأبعاد الزمنية (يوم، شهر، سنة).
```