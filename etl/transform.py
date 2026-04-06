import pandas as pd
import json

def read_file(category):
    with open(f"./data/raw/{category}_data.json", "r") as file:
        data = json.load(file)
        return data
    
def save_as_csv(data, category):
    pass
    
def transform_product_data(data):
    #Q: is there better way to solve this issue
    #A: yes, rewrite it like in carts or user function
    df = pd.DataFrame(data)
    rating_param = df["rating"].apply(pd.Series)
    df = pd.concat([df.drop(["rating"], axis=1), rating_param], axis=1)

    data_col = df.columns
    imp_data_col = data_col[:3]

    #reconcider if writing this logic is needed if most of params are required
    for col_name in data_col:
        mask = df[col_name].isnull()
        if any(mask) and col_name in imp_data_col:
            print(f"==== Invalid product data: ====")
            print(f"Found {sum(mask)} product(s) missing {col_name}:\n{df[mask]}")
            df = df[mask == False]
        elif any(mask):
            print(f"==== No data avilable: ====")
            print(f"Found {sum(mask)} product(s) missing {col_name}:\n{df[mask]}")
            #add changing values in this part
    
    return df

def transform_carts_data(data):
    df = pd.json_normalize(data, record_path="products", meta=["id", "userId", "date"])
    
    #add handling null values

    return df

def transform_users_data(data):
    df = pd.DataFrame(data)
    df = pd.json_normalize(df["address"])
    print(df)
    print(df.columns)
    
    


def main():
    categories = ["products", "carts", "users"]
    # for category in categories:
    #     data = extract.fetch_data(category.upper())
    #     extract.save_as_file(data, category)

    #transform_product_data(read_file(categories[0]))
    #transform_carts_data(read_file(categories[1]))
    transform_users_data(read_file(categories[2]))

if __name__ == "__main__":
    main()