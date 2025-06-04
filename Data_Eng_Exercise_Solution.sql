
-- Step 0: Set Up the Database
CREATE DATABASE OlistEcommerce;
GO

USE OlistEcommerce;
GO

-- Step 1: Table Creation

CREATE TABLE Customers (
    customer_id NVARCHAR(50) PRIMARY KEY,
    customer_zip_code_prefix NVARCHAR(20),
    customer_city NVARCHAR(50),
    customer_state NVARCHAR(10)
);

CREATE TABLE Sellers (
    seller_id NVARCHAR(50) PRIMARY KEY,
    seller_zip_code_prefix NVARCHAR(20),
    seller_city NVARCHAR(50),
    seller_state NVARCHAR(10)
);

CREATE TABLE Products (
    product_id NVARCHAR(50) PRIMARY KEY,
    product_category_name NVARCHAR(50),
    product_name_length INT,
    product_description_length INT,
    product_photos_qty INT,
    product_weight_g FLOAT,
    product_length_cm FLOAT,
    product_height_cm FLOAT,
    product_width_cm FLOAT
);

CREATE TABLE Orders (
    order_id NVARCHAR(50) PRIMARY KEY,
    customer_id NVARCHAR(50),
    order_status NVARCHAR(20),
    order_purchase_timestamp DATETIME,
    order_approved_at DATETIME,
    order_delivered_carrier_date DATETIME,
    order_delivered_customer_date DATETIME,
    order_estimated_delivery_date DATETIME,
    FOREIGN KEY (customer_id) REFERENCES Customers(customer_id)
);

CREATE TABLE Order_Items (
    order_id NVARCHAR(50),
    order_item_id INT,
    product_id NVARCHAR(50),
    seller_id NVARCHAR(50),
    shipping_limit_date DATETIME,
    price DECIMAL(10, 2),
    freight_value DECIMAL(10, 2),
    PRIMARY KEY (order_id, order_item_id),
    FOREIGN KEY (order_id) REFERENCES Orders(order_id),
    FOREIGN KEY (product_id) REFERENCES Products(product_id),
    FOREIGN KEY (seller_id) REFERENCES Sellers(seller_id)
);

CREATE TABLE Order_Payments (
    order_id NVARCHAR(50),
    payment_sequential INT,
    payment_type NVARCHAR(50),
    payment_installments INT,
    payment_value DECIMAL(10, 2),
    FOREIGN KEY (order_id) REFERENCES Orders(order_id)
);

CREATE TABLE Order_Reviews (
    review_id NVARCHAR(50) PRIMARY KEY,
    order_id NVARCHAR(50),
    review_score INT,
    review_comment_title NVARCHAR(MAX),
    review_comment_message NVARCHAR(MAX),
    review_creation_date DATETIME,
    review_answer_timestamp DATETIME,
    FOREIGN KEY (order_id) REFERENCES Orders(order_id)
);


-- Step 2: Data Cleaning

WITH Duplicates AS (
    SELECT order_id,
           ROW_NUMBER() OVER (PARTITION BY order_id ORDER BY order_id) AS rn
    FROM Orders
)
DELETE FROM Orders
WHERE order_id IN (SELECT order_id FROM Duplicates WHERE rn > 1);


-- Handle missing values
UPDATE Orders SET order_status =  ISNULL(order_status, 'Unknown');
UPDATE Customers SET customer_city = 'Unknown' WHERE customer_city IS NULL;
UPDATE Products SET product_category_name = 'Unknown' WHERE product_category_name IS NULL;

-- Replace NULL numeric values with 0
UPDATE Order_Items  SET price = 0 WHERE price IS NULL;
UPDATE Order_Items SET freight_value = 0 WHERE freight_value IS NULL;
UPDATE Order_Payments SET payment_value = 0 WHERE payment_value IS NULL;



--Optimization
CREATE NONCLUSTERED INDEX idx_orders_customer_id ON Orders(customer_id);
CREATE NONCLUSTERED INDEX idx_orderitems_product_id ON Order_Items(product_id);
CREATE NONCLUSTERED INDEX idx_orderitems_order_id ON Order_Items(order_id);

 -- Step 3: Create FactOrderItems with Partition

--Create Partition
CREATE PARTITION FUNCTION pf_OrderYearRange (DATE)
AS RANGE RIGHT FOR VALUES ('2017-01-01', '2018-01-01', '2019-01-01');

CREATE PARTITION SCHEME ps_OrderYearScheme
AS PARTITION pf_OrderYearRange ALL TO ([PRIMARY]);


CREATE TABLE FactOrderItems (
    order_item_id INT,
    order_id NVARCHAR(50),
    product_id NVARCHAR(50),
    seller_id NVARCHAR(50),
    total_price FLOAT,
    delivery_time INT,
    payment_count INT,
    profit_margin FLOAT,
    order_purchase_timestamp DATETIME)
	ON ps_OrderYearScheme(order_purchase_timestamp);

-- Create Index
CREATE CLUSTERED INDEX idx_fact_order_item_id ON FactOrderItems(order_item_id);
CREATE NONCLUSTERED INDEX idx_fact_order_id ON FactOrderItems(order_id);


---Create procedure to calculate  data and store in a fact table
CREATE PROCEDURE PR_InsertFactOrderItems
AS
BEGIN
    SET NOCOUNT ON;

    BEGIN TRY
        INSERT INTO FactOrderItems
        SELECT 
            a.order_item_id,
            a.order_id,
            a.product_id,
            a.seller_id,
            a.price + a.freight_value AS total_price,
            DATEDIFF(DAY, b.order_purchase_timestamp, b.order_delivered_customer_date) AS delivery_time,
            ISNULL(MAX(c.payment_installments), 0) AS payment_count,
            a.price - a.freight_value AS profit_margin,
            b.order_purchase_timestamp
        FROM dbo.Order_Items a
        JOIN dbo.Orders b ON a.order_id = b.order_id
        LEFT JOIN dbo.Order_Payments c ON a.order_id = c.order_id
        WHERE NOT EXISTS (
            SELECT 1 FROM dbo.FactOrderItems f
            WHERE f.order_id = a.order_id AND f.order_item_id = a.order_item_id)
        GROUP BY 
            a.order_item_id, a.order_id, a.product_id, a.seller_id, 
            a.price, a.freight_value, 
            b.order_purchase_timestamp, b.order_delivered_customer_date;
    END TRY
    BEGIN CATCH
        PRINT ERROR_MESSAGE();
    END CATCH
END;
GO


EXEC PR_InsertFactOrderItems;


-- Step 4: Window Functions 
SELECT 
    t.customer_id,
    SUM(p.price) OVER (PARTITION BY t.customer_id ORDER BY t.order_purchase_timestamp) AS running_total
INTO TotalSalesPerCustomer
FROM Order_Items p
JOIN Orders t ON p.order_id = t.order_id;

SELECT 
    p.product_category_name,
    AVG(DATEDIFF(DAY, o.order_purchase_timestamp, o.order_delivered_customer_date)) 
        OVER (PARTITION BY p.product_category_name) AS avg_delivery_time
INTO AvgDeliveryTimePerCategory
FROM Order_Items t
JOIN Products p ON t.product_id = p.product_id
JOIN Orders o ON t.order_id = o.order_id;


--(Alternative solution for the first query using indexed view)

CREATE VIEW dbo.vw_DailySalesPerCustomer
WITH SCHEMABINDING
AS
SELECT 
    t.customer_id,
    CONVERT(DATE, t.order_purchase_timestamp) AS order_date,
    COUNT_BIG(*) AS order_count,
    SUM(p.price) AS daily_total
FROM dbo.Order_Items AS p
JOIN dbo.Orders AS t ON p.order_id = t.order_id
GROUP BY t.customer_id, CONVERT(DATE, t.order_purchase_timestamp);
GO

CREATE UNIQUE CLUSTERED INDEX idx_vw_DailySalesPerCustomer
ON dbo.vw_DailySalesPerCustomer(customer_id, order_date);

SELECT 
    customer_id,
    order_date,
    SUM(daily_total) OVER (PARTITION BY customer_id ORDER BY order_date) AS running_total
FROM dbo.vw_DailySalesPerCustomer;

-- Step 5: Dimension Tables
SELECT DISTINCT customer_id, customer_zip_code_prefix, customer_city, customer_state
INTO DimCustomers
FROM Customers;


SELECT DISTINCT product_id, product_category_name, product_weight_g, product_length_cm, product_height_cm, product_width_cm
INTO DimProducts
FROM Products;

SELECT DISTINCT seller_id, seller_zip_code_prefix, seller_city, seller_state
INTO DimSellers
FROM Sellers;

SELECT DISTINCT 
    CAST(order_purchase_timestamp AS DATE) AS date,
    YEAR(order_purchase_timestamp) AS year,
    MONTH(order_purchase_timestamp) AS month,
    DAY(order_purchase_timestamp) AS day
INTO DimDate
FROM Orders;

-- Step 6: Validation Queries
SELECT p.product_category_name, SUM(f.total_price) AS total_sales
FROM FactOrderItems f
JOIN Products p ON f.product_id = p.product_id
GROUP BY p.product_category_name;

SELECT s.seller_id, AVG(f.delivery_time) AS avg_delivery_time
FROM FactOrderItems f
JOIN Sellers s ON f.seller_id = s.seller_id
GROUP BY s.seller_id;

SELECT c.customer_state, COUNT(DISTINCT o.order_id) AS total_orders
FROM Orders o
JOIN Customers c ON o.customer_id = c.customer_id
GROUP BY c.customer_state;
