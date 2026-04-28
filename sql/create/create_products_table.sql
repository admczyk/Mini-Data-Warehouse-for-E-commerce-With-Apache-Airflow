CREATE TABLE IF NOT EXISTS products (
    product_id INT PRIMARY KEY,
    title TEXT NOT NULL,
    description TEXT,
    category TEXT NOT NULL,
    price NUMERIC NOT NULL,
    discount_percentage NUMERIC,
    overall_rating NUMERIC,
    stock INT NOT NULL,
    brand TEXT,
    final_price NUMERIC NOT NULL,
    price_bucket TEXT,
    rating_bucket TEXT,
    value_score NUMERIC
);