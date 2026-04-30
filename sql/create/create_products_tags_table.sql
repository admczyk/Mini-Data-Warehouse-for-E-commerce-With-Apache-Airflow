CREATE TABLE IF NOT EXISTS products_tags (
    tag_id INT PRIMARY KEY,
    product_id INT NOT NULL,
    tag TEXT NOT NULL,
    FOREIGN KEY (product_id) REFERENCES products(product_id)
);