from airflow.decorators import task
import requests
import json
from airflow.models import Variable

WEBSITE_PATH = Variable.get("WEBSITE_PATH")

@task
def get_data(category):
    """
    Extracts data from website using API_KEY provided in config.py file

    Args:
        category (string): sets one of three possible categories of API_KEY 
                            (products/carts/users)

    Returns:
        list: data about set category
    """
    url = f"{WEBSITE_PATH}{category}"
    
    try:
        response = requests.get(url)
        response.raise_for_status()

        data = response.json()
        return data
    except Exception as e:
        raise e

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
    
    with open(f"/opt/airflow/data/raw/{category}_data.json", "w") as file:
        json.dump(data, file, indent=2)
