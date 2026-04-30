INSERT INTO products (
    product_id, title, description, category, price,
    discount_percentage, overall_rating, stock, brand,       
    final_price, price_bucket, rating_bucket, value_score
) VALUES %s
ON CONFLICT (product_id) DO NOTHING;