import pandas as pd
import json

def read_file(category):
    try:
        with open(f"./data/raw/{category}_data.json", "r") as file:
            data = json.load(file)
            return data
    except Exception as e:
        print(f"Unexpected {e}.")
    
def save_as_csv(data, category):
    try:
        data.to_csv(f"./data/transformed/{category}_transformed_data.csv", index=False)
    except Exception as e:
        print(f"Unexpected {e}.")

def transform_product_data(data):
    df = pd.DataFrame(data)
    try:
        rating_df = pd.json_normalize(df["rating"])
        df = pd.concat([df.drop(["rating"], axis=1), rating_df], axis=1)
        print(df)

        data_col = df.columns
        imp_data_col = data_col[:len(data_col)-2]

        for col_name in data_col:
            mask = df[col_name].isnull()
            if any(mask) and col_name in imp_data_col:
                print("========== Invalid product data: ==========")
                print(f"Found {sum(mask)} product(s) missing {col_name}:\n{df[mask]}")
                print("WARNING: Products with invalid product data will not be included in final database.\n")
                df = df[mask == False]
            elif any(mask):
                print("========== No data avilable: ==========")
                print(f"Found {sum(mask)} product(s) missing {col_name}:\n{df[mask]}")
                df.loc[mask, col_name] = 0
        
        df = df.reset_index(drop=True)

        df["id"] = df["id"].astype(int)
        df["title"] = df["title"].astype(str)
        df["price"] = df["price"].astype(float)
        df["description"] = df["description"].astype(str)
        df["category"] = df["category"].astype(str)
        df["image"] = df["image"].astype(str)
        df["rate"] = df["rate"].astype(float)
        df["count"] = df["count"].astype(int)

        return df
    except Exception as e:
        print(f"Unexpected {e}.")

def transform_carts_data(data):
    df = pd.json_normalize(data, record_path="products", meta=["id", "userId", "date"])
    print(df)
    #add handling null values
    #change to correct variable type

    return df

def transform_users_data(data):
    df = pd.DataFrame(data)
    address_df = pd.json_normalize(df["address"])
    name_df = pd.json_normalize(df["name"])
    df = pd.concat([df.drop(["address", "name"], axis=1), name_df, address_df], axis=1)
    df = df.rename(columns={
        "geolocation.lat": "latitude",
        "geolocation.long": "longitude"
    })


    print(df)    
    


def main():
    categories = ["products", "carts", "users"]
    # for category in categories:
    #     data = extract.fetch_data(category.upper())
    #     extract.save_as_file(data, category)

    transform_product_data(read_file(categories[0]))
    #transform_carts_data(read_file(categories[1]))
    #transform_users_data(read_file(categories[2]))

if __name__ == "__main__":
    main()