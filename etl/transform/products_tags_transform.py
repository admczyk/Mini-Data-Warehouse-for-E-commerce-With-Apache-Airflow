import pandas as pd
import datetime as dt
import json

def extract_product_tags(tags_df):
    tags_df = tags_df.explode("tags")
    tags_df = tags_df.reset_index(drop=True)
    tags_df["tags"] = tags_df["tags"].astype("category")

    final_tags_df = pd.DataFrame()
    final_tags_df["tag_id"] = tags_df["tags"].cat.codes + 1
    final_tags_df = final_tags_df.join(tags_df)

    return final_tags_df