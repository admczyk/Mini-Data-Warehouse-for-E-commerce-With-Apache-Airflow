CREATE TABLE IF NOT EXISTS products_reviews (
    review_id INT PRIMARY KEY,
    product_id INT NOT NULL,
    review_comment TEXT,
    review_date DATE,
    rating NUMERIC NOT NULL,
    review_year INT,
    review_month INT,
    review_bucket TEXT,
    FOREIGN KEY (product_id) REFERENCES products(product_id)
);