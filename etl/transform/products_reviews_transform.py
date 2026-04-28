import pandas as pd
import datetime as dt
import json

def extract_product_reviews(reviews_df):
    # otpional : figuere out how to assign user_id to every review
    reviews_df = reviews_df.explode("reviews")
    print(reviews_df)
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