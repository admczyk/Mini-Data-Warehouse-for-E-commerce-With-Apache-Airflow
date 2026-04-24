import pandas as pd
import json

def read_file(category):
    try:
        with open(f"./data/raw/test_{category}_data.json", "r") as file:
            data = json.load(file)
            return data
    except Exception as e:
        raise e
    
def transform_carts_data(data):
    pass

def main():
    data = read_file("products")

    result = transform_carts_data(data)

if __name__ == "__main__":
    main()