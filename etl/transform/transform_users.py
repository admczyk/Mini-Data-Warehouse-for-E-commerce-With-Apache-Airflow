import pandas as pd
import json

def read_file(category):
    try:
        with open(f"./data/raw/test_{category}_data.json", "r") as file:
            data = json.load(file)
            return data
    except Exception as e:
        raise e
    
def transform_users_data(data):
    users_df = pd.DataFrame(data)
    
    users_df = users_df.drop(columns=["age"])

    users_df = users_df.rename(columns={
        "firstName": "first_name",
        "lastName": "last_name",
        "birthDate": "birth_date",
        "postalCode": "postal_code"
    })

    return users_df

def main():
    data = read_file("users")

    result = transform_users_data(data)
    print(result)

if __name__ == "__main__":
    main()