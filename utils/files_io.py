from airflow.decorators import task
import pandas as pd
import json

# extract.py
@task
def save_to_json(data, category):
    """
    Saves provided data to a JSON file

    Args:
        data (list): data saved to JSON file
        category (string): sets one of three possible filenames
                            (products/carts/users)
    
    Returns:
        None
    """
    try:
        with open(f"/opt/airflow/data/raw/{category}_data.json", "w") as file:
            json.dump(data, file, indent=2)
    except Exception as e:
        raise e
    
# transform_*.py
@task
def read_json(category):
    try:
        #with open(f"./data/raw/{category}_data.json", "r") as file:
        with open(f"/opt/airflow/data/raw/{category}_data.json", "r") as file:
            data = json.load(file)
            return data
    except Exception as e:
        raise e
    
@task 
def save_as_csv(data, category):
    # data.to_csv(f"./data/transformed/{category}_transformed_data.csv", index=False)
    data.to_csv(f"/opt/airflow/data/transformed/{category}_transformed_data.csv", index=False)
    
# load.py
@task
def read_csv(category):
    pd.read_csv(f"./data/transformed/{category}_transformed_data.csv")