import requests
from dotenv import load_dotenv
import os
import json

load_dotenv()

def fetch_data(category):
    url = os.getenv(f"{category}_API_KEY")
    
    try:
        response = requests.get(url)
        response.raise_for_status()

        data = response.json()
        return data
    except Exception as e:
        print(f"Uh oh: {e}")

def save_as_file(dict, category):
    with open(f"./data/raw/{category}_data.json", "w") as file:
        json.dump(dict, file, indent=2)
