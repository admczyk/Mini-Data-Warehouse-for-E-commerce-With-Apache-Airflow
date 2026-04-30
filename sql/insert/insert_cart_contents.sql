INSERT INTO carts_contents (
    cart_id, product_id, product_price, product_quantity,
    product_total, product_total_discounted
) VALUES %s
ON CONFLICT (cart_id) DO NOTHING;