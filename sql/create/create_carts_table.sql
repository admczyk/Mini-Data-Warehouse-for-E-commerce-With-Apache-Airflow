CREATE TABLE IF NOT EXISTS carts (
    cart_id INT PRIMARY KEY,
    cart_total NUMERIC NOT NULL,
    cart_total_discounted NUMERIC NOT NULL,
    user_id INT NOT NULL,
    cart_total_quantity INT NOT NULL,
    cart_size_bucket TEXT,
    cart_value_bucket TEXT,
    FOREIGN KEY (user_id) REFERENCES users(user_id)
);