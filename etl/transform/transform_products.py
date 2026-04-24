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

def extract_product_reviews(products_df):
    reviews_df = products_df[["product_id", "reviews"]].explode("reviews")
    reviews_df = pd.concat(
            [reviews_df.drop(columns=["reviews"]).reset_index(drop=True),
             pd.json_normalize(reviews_df["reviews"].reset_index(drop=True))],
             axis = 1
        )
    reviews_df = reviews_df.drop(columns=["reviewerName", "reviewerEmail"])
    reviews_df = reviews_df.reset_index()
    reviews_df["index"] = reviews_df["index"] + 1

    reviews_df = reviews_df.rename(columns={"index": "review_id"})

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

def add_new_product_values(data):
    data["final_price"] = data["price"] * (1-data["discount_percentage"])
    data["price_bucket"] = pd.cut(
        data["price"],
        bins=[0, 100, 500, 2000, 10000, float("inf")],
        labels=["very_low", "low", "medium", "high", "premium"]
    )
    data["rating_bucket"] = pd.cut(
        data["overall_rating"],
        bins=[0, 2, 3, 4, 5],
        labels=["bad", "neutral", "good", "excellent"]
    )
    data["value_score"] = data["overall_rating"] / data["price"]
    return data

def add_new_reviews_values(data):
    data["date"] = pd.to_datetime(data["date"])
    data["review_year"] = data["date"].dt.year
    data["review_month"] = data["date"].dt.month
    data["review_bucket"] = pd.cut(
        data["rating"],
        bins=[0, 2, 3, 4, 5],
        labels=["bad", "neutral", "good", "excellent"]
    )
    print(data)

def transform_product_data(data):
    products_df = pd.DataFrame(data)

    products_df = products_df["products"]
    products_df = pd.json_normalize(products_df)

    products_df = products_df.drop(columns=[
        "sku", "weight", "warrantyInformation", "shippingInformation",
        "returnPolicy", "images", "thumbnail", "dimensions.width",
        "dimensions.height", "dimensions.depth", "meta.createdAt",
        "meta.updatedAt", "meta.barcode", "meta.qrCode", 
        "availabilityStatus", "minimumOrderQuantity"
    ])
    
    products_df = products_df.rename(columns={
        "id": "product_id",
        "discountPercentage": "discount_percentage",
        "rating": "overall_rating"
    })
    
    tags_df = products_df[["product_id", "tags"]].explode("tags")
    products_df = products_df.drop(columns=["tags"])
    products_df = products_df.merge(tags_df, on="product_id", how="left")

    reviews_df = extract_product_reviews(products_df)
    products_df = products_df.drop(columns=["reviews"])
    
    # checks missing values
    print(products_df.isna().sum())
    products_df = check_missing_values(products_df)
    
    # creates new columns useful for analysis
    products_df = add_new_product_values(products_df)
    reviews_df = add_new_reviews_values(reviews_df)

    return products_df, reviews_df
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

    products, reviews = transform_product_data(data)
    print(products)
    print(reviews)

    print(products.columns)
    print(reviews.columns)

    print(products["price"].min())
    print(products["price"].max())

    print(reviews.dtypes)


if __name__ == "__main__":
    main()

