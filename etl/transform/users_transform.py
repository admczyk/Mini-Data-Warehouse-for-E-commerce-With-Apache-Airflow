import pandas as pd
import datetime as dt
import json

def read_file(category):
    try:
        with open(f"./data/raw/test_{category}_data.json", "r") as file:
            data = json.load(file)
            return data
    except Exception as e:
        raise e
    
def add_new_users_values(data):
    data["birth_date"] = pd.to_datetime(data["birth_date"])
    data["age_group"] = pd.cut(
        (pd.Timestamp.today().normalize() - data["birth_date"]).dt.days // 365,
        bins=[17, 24, 34, 44, 54, 64, 120],
        labels=["18-24", "25-34", "35-44", "45-54", "55-64", "65+"]
    )
    return data
    
def transform_users_data(data):
    users_df = pd.DataFrame(data)
    
    users_df = users_df.drop(columns=["age"])

    users_df = users_df.rename(columns={
        "firstName": "first_name",
        "lastName": "last_name",
        "birthDate": "birth_date",
        "postalCode": "postal_code"
    })

    users_df = add_new_users_values(users_df)

    return users_df

def main():
    data = read_file("users")

    result = transform_users_data(data)
    print(result)
    print(result.columns)
    print(result.dtypes)

if __name__ == "__main__":
    main()