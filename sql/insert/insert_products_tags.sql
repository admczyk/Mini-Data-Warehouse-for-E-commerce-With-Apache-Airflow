INSERT INTO products_tags (
    tag_id, product_id, tag
) VALUES %s
ON CONFLICT (tag_id) DO NOTHING;