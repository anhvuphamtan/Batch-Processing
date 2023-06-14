DROP SCHEMA IF EXISTS warehouse_sales CASCADE;

CREATE SCHEMA warehouse_sales;

CREATE TABLE warehouse_sales.Sales (
    Sale_ID VARCHAR(255) PRIMARY KEY,
    Revenue DECIMAL(10, 3),
    Profit DECIMAL(10, 3),
    Quantity INT,
    Shipping_cost DECIMAL(10, 3),

    Product_ID BIGINT,
    Customer_ID VARCHAR(255),
    Shipping_zipcode INT,
    Order_date DATE,
    Shipment_ID VARCHAR(255)
);

CREATE TABLE IF NOT EXISTS warehouse_sales.Products (
    Product_ID BIGINT PRIMARY KEY,
    Product_name VARCHAR(255),
    SKU INT,
    Brand VARCHAR(255),
    Category VARCHAR(255),
    Product_size DECIMAL
);

CREATE TABLE IF NOT EXISTS warehouse_sales.Shipments (
    Shipment_ID VARCHAR(255) PRIMARY KEY,
    Shipping_mode VARCHAR(255),
    Shipping_status VARCHAR(255),
    Shipping_company VARCHAR(255)
);

CREATE TABLE IF NOT EXISTS warehouse_sales.Customers (
    Customer_ID VARCHAR(255) PRIMARY KEY,
    Name VARCHAR(255),
    Phone VARCHAR(255),
    Age INT
);

CREATE TABLE IF NOT EXISTS warehouse_sales.Locations (
    Shipping_zipcode INT PRIMARY KEY,
    City VARCHAR(45),
    State VARCHAR(45),
    Country VARCHAR(45),
    Shipping_address VARCHAR(255)
);

CREATE TABLE IF NOT EXISTS warehouse_sales.Time (
    Full_date Date PRIMARY KEY,
    Day INT,
    Month INT,
    Year INT
);

ALTER TABLE warehouse_sales.Sales 
ADD CONSTRAINT fk_sale_product_prodID FOREIGN KEY (Product_ID)
REFERENCES warehouse_sales.Products (Product_ID);

ALTER TABLE warehouse_sales.Sales 
ADD CONSTRAINT fk_sale_customer_custID FOREIGN KEY (Customer_ID)
REFERENCES warehouse_sales.Customers (Customer_ID);

ALTER TABLE warehouse_sales.Sales 
ADD CONSTRAINT fk_sale_customer_shipmentID FOREIGN KEY (Shipment_ID)
REFERENCES warehouse_sales.Shipments (Shipment_ID);

ALTER TABLE warehouse_sales.Sales 
ADD CONSTRAINT fk_sale_customer_zipcode FOREIGN KEY (Shipping_zipcode)
REFERENCES warehouse_sales.Locations (Shipping_zipcode);

ALTER TABLE warehouse_sales.Sales 
ADD CONSTRAINT fk_sale_customer_timeID FOREIGN KEY (Order_date)
REFERENCES warehouse_sales.Time (Full_date);