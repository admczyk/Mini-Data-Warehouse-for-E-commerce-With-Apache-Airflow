CREATE TABLE IF NOT EXISTS carts_contents (
    cart_id INT NOT NULL,
    product_id INT NOT NULL,
    product_price NUMERIC NOT NULL,
    product_quantity INT NOT NULL,
    product_total NUMERIC NOT NULL,
    product_total_discounted NUMERIC NOT NULL,
    PRIMARY KEY (cart_id, product_id),
    FOREIGN KEY (cart_id) REFERENCES carts(cart_id),
    FOREIGN KEY (product_id) REFERENCES products(product_id)
);