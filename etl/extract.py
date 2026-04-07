import requests
from dotenv import load_dotenv
import os
import json

load_dotenv()

def fetch_data(category):
    """
    Extracts data from website using API_KEY provided in config.py file

    Args:
        category (string): sets one of three possible categories of API_KEY 
                            (products/carts/users)

    Returns:
        dict: data about set category
    """
    url = os.getenv(f"{category}_API_KEY")
    
    try:
        response = requests.get(url)
        response.raise_for_status()

        data = response.json()
        return data
    except Exception as e:
        print(f"Uh oh: {e}")

def save_as_json(dict, category):
    """
    Saves provided data to a JSON file

    Args:
        dict (dict): data saved in JSON file
        category (string): sets one of three possible filenames
                            (products/carts/users)
    
    Returns:
        None
    """
    with open(f"./data/raw/{category}_data.json", "w") as file:
        json.dump(dict, file, indent=2)
