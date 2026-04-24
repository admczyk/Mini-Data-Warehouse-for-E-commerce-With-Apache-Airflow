#from airflow.decorators import task
import requests
import json
from dotenv import load_dotenv
import os
#from airflow.models import Variable

#WEBSITE_PATH = Variable.get("WEBSITE_PATH")

load_dotenv()

WEBSITE_PATH = os.getenv("WEBSITE_PATH")

#@task
def get_data(category):
    """
    Extracts data from website using API_KEY provided in config.py file

    Args:
        category (string): sets one of three possible categories of API_KEY 
                            (products/carts/users)

    Returns:
        list: data about set category
    """
    url = f"{WEBSITE_PATH}{category}?limit=0"
    
    try:
        response = requests.get(url)
        response.raise_for_status()

        data = response.json()
        return data
    except Exception as e:
        raise e

#@task
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
        with open(f"./data/raw/test_{category}_data.json", "w") as file:
            json.dump(data, file, indent=2)
        # with open(f"/opt/airflow/data/raw/{category}_data.json", "w") as file:
        #     json.dump(data, file, indent=2)
    except Exception as e:
        raise e
    
def main():
    categories = ["products", "carts", "users"]
    for c in categories:
        data = get_data(c)
        save_to_json(data, c)

if __name__ == "__main__":
    main()
