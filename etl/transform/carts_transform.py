import pandas as pd
import json

def read_file(category):
    try:
        with open(f"./data/raw/test_{category}_data.json", "r") as file:
            data = json.load(file)
            return data
    except Exception as e:
        raise e
    
def extract_cart_contents(carts_df):
    cart_products_df = carts_df[["cart_id", "products"]].explode("products").reset_index(drop=True)
    products_df = pd.json_normalize(cart_products_df["products"])

    products_df = products_df.rename(columns={
        "id": "product_id", 
        "total": "product_total",
        "price": "product_price",
        "quantity": "product_quantity",
        "discountedTotal": "product_total_discounted"
    })
    products_df = products_df.drop(columns=["title", "thumbnail", "discountPercentage"])

    cart_products_df = cart_products_df.drop(columns=["products"]).reset_index(drop=True)
    cart_products_df = cart_products_df.join(products_df)

    return cart_products_df

def add_new_cart_values(data):
    data["cart_size_bucket"] = pd.cut(
        data["cart_total_quantity"],
        bins=[0, 3, 6, 12, 20],
        labels=["very_small", "small", "medium", "large"]
    )
    data["cart_value_bucket"] = pd.cut(
        data["cart_total_discounted"],
        bins=[0, 20, 100, 500, 2000, 10000, float("inf")],
        labels=["micro", "very_low", "low", "medium", "large", "enterprise"]
    )
    return data

def transform_carts_data(data):
    carts_df = pd.DataFrame(data)

    carts_df = carts_df["carts"]
    carts_df = pd.json_normalize(carts_df)

    carts_df = carts_df.rename(columns={
        "id": "cart_id",
        "total": "cart_total",
        "userId": "user_id",
        "discountedTotal": "cart_total_discounted",
        "totalQuantity": "cart_total_quantity"})

    cart_products_df = extract_cart_contents(carts_df)
    
    carts_df = carts_df.drop(columns=["products", "totalProducts"])

    carts_df = add_new_cart_values(carts_df)

    # returns two dataframes
    return carts_df, cart_products_df

def main():
    data = read_file("carts")

    carts, cart_products = transform_carts_data(data)
    print(carts)
    print(cart_products)
    print(carts.columns)
    print(cart_products.columns)
    print(carts["cart_total_discounted"].min())
    print(carts["cart_total_discounted"].max())
    print(carts["cart_value_bucket"].value_counts())

if __name__ == "__main__":
    main()