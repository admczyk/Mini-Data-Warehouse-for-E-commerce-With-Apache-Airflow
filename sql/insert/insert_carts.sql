INSERT INTO carts (
    cart_id, cart_total, cart_total_discounted,
    user_id, cart_total_quantity, cart_size_bucket,
    cart_value_bucket
) VALUES %s
ON CONFLICT (cart_id) DO NOTHING;