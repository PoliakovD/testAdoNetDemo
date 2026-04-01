CREATE TABLE table_users(
    id INT AUTO_INCREMENT PRIMARY KEY,
    name TEXT NOT NULL,
    is_driver BOOLEAN DEFAULT FALSE
);

CREATE TABLE table_products(
   id INT AUTO_INCREMENT PRIMARY KEY,
   item_name TEXT NOT NULL,
   quantity TEXT,
   price DECIMAL(10, 2) DEFAULT 0.00,
   is_purchased BOOLEAN DEFAULT FALSE,
   user_id INT NULL,
   FOREIGN KEY (user_id) REFERENCES table_users(id)
);