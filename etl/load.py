from sqlalchemy import create_engine
from config import Config_DB as C
import pandas as pd

def get_connection():
    try:
        engine = create_engine(
            f"postgresql+psycopg2://{C.DB_USER}:{C.DB_PASSWORD}@{C.DB_HOST}:{C.DB_PORT}/{C.DB_NAME}"
        )
        return engine
    except Exception as e:
         print(f"Unexpected {e}")

def load_data(path):
    engine = get_connection()
    
    try:
        data = pd.read_csv(path)
        data.to_sql('products', engine, if_exists="append", index=False)
    except Exception as e:
        print(f"Unexpected {e}")