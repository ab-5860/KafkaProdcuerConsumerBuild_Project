CREATE DATABASE kafka_table;

USE kafka_table;

CREATE TABLE products (
    product_id INT PRIMARY KEY,
    product_name VARCHAR(255),
    category VARCHAR(100),
    price FLOAT,  -- Changed from DECIMAL to FLOAT
    last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Insert initial records
INSERT INTO products (product_id, product_name, category, price, last_updated)
VALUES
    (1,'Laptop', 'Electronics', 1200.00, NOW()),
    (2,'Shoes', 'Clothing', 50.00, NOW()),
    (3,'Smartphone', 'Electronics', 800.00, NOW());


INSERT INTO products (product_id, product_name, category, price, last_updated)
VALUES
    (4,'Headphones', 'Electronics', 150.00, NOW()),
    (5,'T-shirt', 'Clothing', 25.00, NOW());
    
INSERT INTO products (product_id, product_name, category, price, last_updated)
VALUES
    (6,'Mobilephones', 'Electronics', 100.00, NOW()),
    (7,'Jeans', 'Clothing', 25.00, NOW()),
    (8,'Capree', 'Clothing', 25.00, NOW()),
    (9,'Trousers', 'Clothing', 25.00, NOW()),
    (10,'Sweaters', 'Clothing', 25.00, NOW());


