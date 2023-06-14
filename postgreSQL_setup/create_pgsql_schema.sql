-- Create Schema `Sale_schema` for postgreSQL database

DROP SCHEMA IF EXISTS Sale_schema CASCADE;

CREATE SCHEMA Sale_schema;

CREATE TABLE IF NOT EXISTS Sale_schema.Sales (
    Order_ID VARCHAR(255) PRIMARY KEY,
    Order_date Date,
    Product_ID BIGINT,
    Style VARCHAR(45),
    Size VARCHAR(45),
    Quantity INT,
    Payment_method VARCHAR(255),
    Total_cost DECIMAL,
    Profit DECIMAL,
    Customer_ID VARCHAR(255)
);

CREATE TABLE IF NOT EXISTS Sale_schema.Products (
    Product_ID BIGINT PRIMARY KEY,
    Product_name VARCHAR(255),
    SKU INT,
    Brand VARCHAR(255),
    Category VARCHAR(255),
    Product_size DECIMAL,
    Sell_price DECIMAL,
    Commision_rate DECIMAL,
    Commision DECIMAL
);

CREATE TABLE IF NOT EXISTS Sale_schema.Customers (
    Customer_ID VARCHAR(255) PRIMARY KEY,
    Name VARCHAR(255),
    Phone VARCHAR(255),
    Age INT,
    Address VARCHAR(255),
    Postal_code INT
);

CREATE TABLE IF NOT EXISTS Sale_schema.Shipments (
    Shipment_ID VARCHAR(255) PRIMARY KEY,
    Order_ID VARCHAR(255),
    Shipping_date Date,
    Shipping_mode VARCHAR(255),
    Shipping_address VARCHAR(255),
    Shipping_status VARCHAR(255),
    Shipping_company VARCHAR(255),
    Shipping_cost DECIMAL,
    Shipping_zipcode INT
);

CREATE TABLE Sale_schema.Locations (
    Postal_code INT PRIMARY KEY,
    City VARCHAR(45),
    State VARCHAR(45),
    Country VARCHAR(45)
);

ALTER TABLE Sale_schema.Sales
ADD CONSTRAINT fk_sale_product_prodID FOREIGN KEY (Product_ID)
REFERENCES Sale_schema.Products (Product_ID)
ON DELETE CASCADE ON UPDATE CASCADE;


ALTER TABLE Sale_schema.Sales
ADD CONSTRAINT fk_sale_customer_custID FOREIGN KEY (Customer_ID)
REFERENCES Sale_schema.Customers (Customer_ID)
ON DELETE CASCADE ON UPDATE CASCADE;

ALTER TABLE Sale_schema.Shipments
ADD CONSTRAINT fk_shipment_sale_orderID FOREIGN KEY (Order_ID)
REFERENCES Sale_schema.Sales (Order_ID)
ON DELETE CASCADE ON UPDATE CASCADE;

ALTER TABLE Sale_schema.Shipments
ADD CONSTRAINT fk_shipment_location_zipcode FOREIGN KEY (Shipping_zipcode)
REFERENCES Sale_schema.Locations (Postal_code)
ON DELETE CASCADE ON UPDATE CASCADE;

ALTER TABLE Sale_schema.Customers
ADD CONSTRAINT fk_customer_location_postcode FOREIGN KEY (Postal_code)
REFERENCES Sale_schema.Locations (Postal_code)
ON DELETE CASCADE ON UPDATE CASCADE;