import requests
from dotenv import load_dotenv
import os
import json
import bcrypt

load_dotenv()

def hash_passwords(data):
    """
    Hashes passwords of users.

    Args:
        data (list): data containing info about users of the website 

    Returns:
        list: almost the same data - only password changed
    """
    for user in data:
        password = user["password"]
        bytes = password.encode('utf-8')
        salt = bcrypt.gensalt()
        hash = bcrypt.hashpw(bytes, salt)
        hashed_str = hash.decode("utf-8")
        user["password"] = hashed_str
    return data

def fetch_data(category):
    """
    Extracts data from website using API_KEY provided in config.py file

    Args:
        category (string): sets one of three possible categories of API_KEY 
                            (products/carts/users)

    Returns:
        list: data about set category
    """
    url = os.getenv(f"{category}_API_KEY")
    
    try:
        response = requests.get(url)
        response.raise_for_status()

        data = response.json()
        if category == "USERS":
            data = hash_passwords(data)
        return data
    except Exception as e:
        print(f"Unexpected: {e}")

def save_as_json(data, category):
    """
    Saves provided data to a JSON file

    Args:
        data (list): data saved to JSON file
        category (string): sets one of three possible filenames
                            (products/carts/users)
    
    Returns:
        None
    """
    with open(f"./data/raw/{category}_data.json", "w") as file:
        json.dump(data, file, indent=2)
