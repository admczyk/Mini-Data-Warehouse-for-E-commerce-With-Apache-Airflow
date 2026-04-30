INSERT INTO products_reviews (
    review_id, product_id, rating, review_comment,
    review_date, review_year, review_month, review_bucket
) VALUES %s
ON CONFLICT (review_id) DO NOTHING;