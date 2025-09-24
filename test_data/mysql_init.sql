-- MySQL test database initialization

CREATE TABLE products (
    id INT AUTO_INCREMENT PRIMARY KEY,
    name VARCHAR(100) NOT NULL,
    price DECIMAL(10,2) NOT NULL,
    category VARCHAR(50),
    stock_quantity INT DEFAULT 0,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE categories (
    id INT AUTO_INCREMENT PRIMARY KEY,
    name VARCHAR(50) UNIQUE NOT NULL,
    description TEXT
);

-- Insert sample data
INSERT INTO categories (name, description) VALUES
    ('Electronics', 'Electronic devices and gadgets'),
    ('Clothing', 'Apparel and accessories'),
    ('Books', 'Books and educational materials');

INSERT INTO products (name, price, category, stock_quantity) VALUES
    ('Laptop Computer', 999.99, 'Electronics', 25),
    ('Smartphone', 599.99, 'Electronics', 50),
    ('T-Shirt', 19.99, 'Clothing', 100),
    ('Programming Book', 49.99, 'Books', 30),
    ('Wireless Headphones', 149.99, 'Electronics', 75);