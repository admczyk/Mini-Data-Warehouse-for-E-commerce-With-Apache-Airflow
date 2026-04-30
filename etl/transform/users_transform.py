from airflow.decorators import task
import pandas as pd
import datetime as dt
    
def add_new_users_values(data):
    data["birth_date"] = pd.to_datetime(data["birth_date"])
    data["age_group"] = pd.cut(
        (pd.Timestamp.today().normalize() - data["birth_date"]).dt.days // 365,
        bins=[17, 24, 34, 44, 54, 64, 120],
        labels=["18-24", "25-34", "35-44", "45-54", "55-64", "65+"]
    )
    return data

@task  
def transform_users_data(data):
    users_df = pd.DataFrame(data)

    users_df = users_df.rename(columns={
        "id": "user_id",
        "firstName": "first_name",
        "lastName": "last_name",
        "birthDate": "birth_date",
        "postalCode": "postal_code"
    })

    users_df = add_new_users_values(users_df)

    return users_df