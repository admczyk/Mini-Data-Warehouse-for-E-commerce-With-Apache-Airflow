import pandas as pd
import json

def read_file(category):
    try:
        with open(f"./data/raw/test_{category}_data.json", "r") as file:
            data = json.load(file)
            return data
    except Exception as e:
        raise e

def extract_product_reviews(products_df):
    reviews_df = products_df[["id", "reviews"]].explode("reviews")
    reviews_df = pd.concat(
            [reviews_df.drop(columns=["reviews"]).reset_index(drop=True),
             pd.json_normalize(reviews_df["reviews"].reset_index(drop=True))],
             axis = 1
        )
    
    # Add handling null values in reviews :3
    return reviews_df

def check_missing_values(df):
    # IMPORTANT - handling missing values:
    # Numeric:
    #   For <5% missing - drop values 
    #   For 5-20% missing - use median/mean
    #   For >20% missing - use advanced model-based imputation
    # String:
    #   If is reqired param (id) - drop value
    #   If required param and not that important (title, desc,category) - create own category "missing_x"
    #   If isn't required param (brand, tags) - create own category (ex. "not set")
    #   If else (availabilityStatus) - check with other data and then decide if assign value Y/N or delete
    #   
    # AT THE END ADD LOGS OF MISSING VALUES, SO THEY CAN BE FIXED 
    # REQUIRED PARAMS: id (PRIM KEY), title, price, category, stock, availabilityStatus
    # OPTIONAL (NULL): description, brand, discountPercentage, tags, rating, minimumOrderQuantity
    return df

def transform_product_data(data):
    products_df = pd.DataFrame(data)

    products_df = products_df["products"]
    products_df = pd.json_normalize(products_df)

    products_df = products_df.drop(columns=[
        "sku", "weight", "warrantyInformation", "shippingInformation",
        "returnPolicy", "images", "thumbnail", "dimensions.width",
        "dimensions.height", "dimensions.depth", "meta.createdAt",
        "meta.updatedAt", "meta.barcode", "meta.qrCode"])

    tags_df = products_df[["id", "tags"]].explode("tags")
    products_df = products_df.drop(columns=["tags"])
    products_df = products_df.merge(tags_df, on="id", how="left")

    reviews_df = extract_product_reviews(products_df)
    products_df = products_df.drop(columns=["reviews"])
    print(reviews_df)

    print(products_df.isna().sum())

    products_df = check_missing_values(products_df)
     
    print(products_df.dtypes)
    # try:
    #     rating_df = pd.json_normalize(df["rating"])
    #     df = pd.concat([df.drop(["rating", "image"], axis=1), rating_df], axis=1)

    #     df = df.rename(columns={"rate": "rating_rate", "count": "rating_count"})
    #     data_col = df.columns
    #     imp_data_col = [c for c in data_col if "rating" not in c]

    #     for col_name in data_col:
    #         mask = df[col_name].isnull()
    #         if any(mask) and col_name in imp_data_col:
    #             print("========== Invalid product data: ==========")
    #             print(f"Found {sum(mask)} product(s) missing {col_name}:\n{df[mask]}")
    #             print("WARNING: Products with invalid product data will not be included in final database.\n")
    #             df = df[mask == False]
    #         elif any(mask):
    #             print("========== No data avilable: ==========")
    #             print(f"Found {sum(mask)} product(s) missing {col_name}:\n{df[mask]}")
    #             df.loc[mask, col_name] = 0
        
    #     df = df.reset_index(drop=True)

    #     df["id"] = df["id"].astype(int)
    #     df["title"] = df["title"].astype(str)
    #     df["price"] = df["price"].astype(float)
    #     df["description"] = df["description"].astype(str)
    #     df["category"] = df["category"].astype(str)
    #     df["rating_rate"] = df["rating_rate"].astype(float)
    #     df["rating_count"] = df["rating_count"].astype(int)

    #     return df
    # except Exception as e:
    #     raise e

def main():
    data = read_file("products")

    result = transform_product_data(data)

if __name__ == "__main__":
    main()

