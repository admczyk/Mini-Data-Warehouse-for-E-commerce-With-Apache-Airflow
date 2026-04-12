import pandas as pd
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.decorators import task

@task
def load_data(category):
    try:
        data = pd.read_csv(f"./data/transformed/{category}_transformed_data.csv")
        

        hook = PostgresHook(postgres_conn_id="postgres_default")
        conn = hook.get_conn()
        cursor = conn.cursor()

        for _, row in data.iterrows():
            cursor.execute("""
                INSERT INTO products (
                    id, title, price, description, category,
                    rating_rate, rating_count, price_category,
                    rating_category, product_segment,
                    avg_price_category, avg_rating_category
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (id) DO NOTHING         
            """, (
                row["id"],
                row["title"],
                row["price"],
                row["description"],
                row["category"],
                row["rating_rate"], 
                row["rating_count"],
                row["price_category"],
                row["rating_category"],
                row["product_segment"],
                row["avg_price_category"],
                row["avg_rating_category"]
            ))

        conn.commit()
        cursor.close()
        conn.close()
    except Exception as e:
        raise e