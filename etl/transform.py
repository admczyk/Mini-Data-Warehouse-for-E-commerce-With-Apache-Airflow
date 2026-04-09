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
        df = pd.concat([df.drop(["rating", "image"], axis=1), rating_df], axis=1)

        df = df.rename(columns={"rate": "rating_rate", "count": "rating_count"})
        data_col = df.columns
        imp_data_col = [c for c in data_col if "rating" not in c]

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
        df["rating_rate"] = df["rating_rate"].astype(float)
        df["rating_count"] = df["rating_count"].astype(int)

        return df
    except Exception as e:
        print(f"Unexpected {e}.")

def products_summary(df):
    try:
        price = [0, 50, 200, 500, 1000]
        labels_price = ["cheap", "moderate", "expensive", "very_expensive"]
        df["price_category"] = pd.cut(df["price"], bins=price, labels=labels_price)
        
        rating = [0, 0.1, 3, 4, 4.5, 5]
        labels_rating = ["no_rating", "poor", "average", "good", "excellent"]
        df["rating_category"] = pd.cut(df["rating_rate"], bins=rating, labels=labels_rating)

        df["product_segment"] = df["price_category"].astype(str) + "_" + df["rating_category"].astype(str)
        df["avg_price_category"] = round(df.groupby("category")["price"].transform("mean"), 2)
        df["avg_rating_category"] = round(df.groupby("category")["rating_rate"].transform("mean"), 2)
        
        return df
    except Exception as e:
        print(f"Unexpected {e}.")