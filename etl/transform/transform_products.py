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
    # otpional : figuere out how to assign user_id to every review
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

def extract_product_tags(products_df):
    tags_df = tags_df.rename(columns={"tags": "tag"})

    tags_df = products_df[["product_id", "tag"]].explode("tag")
    tags_df = tags_df.reset_index(drop=True)
    tags_df["tag"] = tags_df["tag"].astype("category")

    final_tags_df = pd.DataFrame()
    final_tags_df["tag_id"] = tags_df["tag"].cat.codes + 1
    final_tags_df = final_tags_df.join(tags_df)

    return final_tags_df

def validate_product_data(df):
    # A. checking required columns first
    required_cols = ["product_id", "title", "price", "category", "stock"]

    # A1. checking missing values
    missing_mask = df[required_cols].isnull().any(axis=1)
    if missing_mask.any():
        print(f"Deleted {missing_mask.sum()} records with missing reqired values.")
        df = df[~missing_mask]

    # A2. checking if all numeric values are positive
    num_cols = df[required_cols].select_dtypes(include="number")
    neg_mask = (num_cols < 0).any(axis=1)
    if neg_mask.any():
        print(f"Deleted {neg_mask.sum()} records with negative values.")
        df = df[~neg_mask]

    invalid_ids = df["product_id"] <= 0
    if invalid_ids.any():
        print(f"Removed {invalid_ids.sum()} invalid IDs.")
        df = df[~invalid_ids]

    # B. Checking other columns
    # B1. Str
    other_cols = [col for col in df.columns if col not in required_cols]

    str_cols = df[other_cols].select_dtypes(include=["object", "str"])
    missing_mask = str_cols.isnull().any(axis=1)
    if missing_mask.any():
        print(f"Found {missing_mask.sum()} rows with missing strings. Filled them with \"not set\".")
        df[str_cols.columns] = str_cols.fillna("not set")

    # B2. Numeric
    num_cols = df[other_cols].select_dtypes(include="number")
    missing_mask = num_cols.isnull().mean()
    for col, ratio in missing_mask.items():
        if ratio == 0:
            continue

        if ratio < 0.05:
            print(f"Deleted records with missing {col} value.")
            df = df.dropna(subset=[col])
        elif ratio < 0.2:
            print(f"Replacing missing {col} with 0.")
            df[col] = df[col].fillna(0)
    
    df["discount_percentage"] = df["discount_percentage"].clip(0, 100)
    df["overall_rating"] = df["overall_rating"].clip(0, 5)

    dups = df.duplicated(subset=["product_id"])
    if dups.any():
        print(f"Removed {dups.sum()} duplicate rows.")
        df = df[~dups]

    return df

def normalize_dtypes(df):
    pass

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
    return data

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

    # IMPORTANT !!!
    # ADD SEPARATE ID FOR TAGS
    tags_df = extract_product_tags(products_df)
    products_df = products_df.drop(columns=["tags"])

    reviews_df = extract_product_reviews(products_df)
    products_df = products_df.drop(columns=["reviews"])
    
    # checks missing values
    products_df = validate_product_data(products_df)
    
    # creates new columns useful for analysis
    products_df = add_new_product_values(products_df)
    reviews_df = add_new_reviews_values(reviews_df)

    return products_df, reviews_df, tags_df

def main():
    data = read_file("products")

    products, reviews, tags = transform_product_data(data)
    print(products)
    print(reviews)
    print(tags)

    print(products.columns)
    print(reviews.columns)

    # print(products["price"].min())
    # print(products["price"].max())

    print(products.dtypes)
    print(reviews.dtypes)


if __name__ == "__main__":
    main()

