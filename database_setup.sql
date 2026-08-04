-- ========================================================
-- 1. إنشاء جدول الأبعاد الخاص بالتواريخ (Dim_Date)
-- ========================================================
CREATE TABLE Dim_Date (
    Date_ID NUMBER PRIMARY KEY,
    Review_Date DATE,
    Review_Year NUMBER,
    Review_Month NUMBER,
    Review_Day NUMBER
);

CREATE SEQUENCE seq_dim_date START WITH 1 INCREMENT BY 1;

CREATE OR REPLACE TRIGGER trg_dim_date
BEFORE INSERT ON Dim_Date FOR EACH ROW
BEGIN
    SELECT seq_dim_date.NEXTVAL INTO :NEW.Date_ID FROM dual;
END;
/

-- ========================================================
-- 2. إنشاء جدول الأبعاد الخاص بالمنتجات (Dim_Product)
-- ========================================================
CREATE TABLE Dim_Product (
    Product_ID NUMBER PRIMARY KEY,
    Product_Name VARCHAR2(255),
    Category VARCHAR2(100),
    Brand VARCHAR2(100)
);

CREATE SEQUENCE seq_dim_product START WITH 1 INCREMENT BY 1;

CREATE OR REPLACE TRIGGER trg_dim_product
BEFORE INSERT ON Dim_Product FOR EACH ROW
BEGIN
    SELECT seq_dim_product.NEXTVAL INTO :NEW.Product_ID FROM dual;
END;
/

-- ========================================================
-- 3. إنشاء جدول الأبعاد الخاص بالعملاء (Dim_Customer)
-- ========================================================
CREATE TABLE Dim_Customer (
    Customer_ID NUMBER PRIMARY KEY,
    Customer_Name VARCHAR2(255),
    Age NUMBER,
    Gender VARCHAR2(20)
);

CREATE SEQUENCE seq_dim_customer START WITH 1 INCREMENT BY 1;

CREATE OR REPLACE TRIGGER trg_dim_customer
BEFORE INSERT ON Dim_Customer FOR EACH ROW
BEGIN
    SELECT seq_dim_customer.NEXTVAL INTO :NEW.Customer_ID FROM dual;
END;
/

-- ========================================================
-- 4. إنشاء جدول الأبعاد الإضافي للموقع الجغرافي (Dim_Location)
-- ========================================================
CREATE TABLE Dim_Location (
    Location_ID NUMBER PRIMARY KEY,
    Country VARCHAR2(100),
    City VARCHAR2(100)
);

CREATE SEQUENCE seq_dim_location START WITH 1 INCREMENT BY 1;

CREATE OR REPLACE TRIGGER trg_dim_location
BEFORE INSERT ON Dim_Location FOR EACH ROW
BEGIN
    SELECT seq_dim_location.NEXTVAL INTO :NEW.Location_ID FROM dual;
END;
/

-- ========================================================
-- 5. إنشاء الجدول المركزي للحقائق (Fact_Reviews)
-- ========================================================
CREATE TABLE Fact_Reviews (
    Review_ID NUMBER PRIMARY KEY,
    Product_ID NUMBER,
    Customer_ID NUMBER,
    Date_ID NUMBER,
    Location_ID NUMBER,
    Review_Text CLOB,
    Sentiment_Label VARCHAR2(20),
    Issue_Reason VARCHAR2(500),
    AI_Recommendation VARCHAR2(500),
    Source_Link VARCHAR2(500),
    Load_Timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    
    CONSTRAINT fk_fact_product FOREIGN KEY (Product_ID) REFERENCES Dim_Product(Product_ID),
    CONSTRAINT fk_fact_customer FOREIGN KEY (Customer_ID) REFERENCES Dim_Customer(Customer_ID),
    CONSTRAINT fk_fact_date FOREIGN KEY (Date_ID) REFERENCES Dim_Date(Date_ID),
    CONSTRAINT fk_fact_location FOREIGN KEY (Location_ID) REFERENCES Dim_Location(Location_ID)
);

CREATE SEQUENCE seq_fact_reviews START WITH 1 INCREMENT BY 1;

CREATE OR REPLACE TRIGGER trg_fact_reviews
BEFORE INSERT ON Fact_Reviews FOR EACH ROW
BEGIN
    SELECT seq_fact_reviews.NEXTVAL INTO :NEW.Review_ID FROM dual;
END;
/