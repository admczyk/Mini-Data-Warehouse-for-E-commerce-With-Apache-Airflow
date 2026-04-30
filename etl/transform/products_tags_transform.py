from airflow.decorators import task
import pandas as pd

def normalize_tags_dtypes(tags_df):
    tags_df["product_id"] = pd.to_numeric(tags_df["product_id"], errors="coerce")
    tags_df["tag"] = tags_df["tag"].astype("category")

    return tags_df

def clean_and_validate_tags_data(tags_df):
    required_cols = tags_df.columns

    missing_mask = tags_df[required_cols].isnull().any(axis=1)
    if missing_mask.any():
        print(f"Deleted {missing_mask.sum()} records with missing reqired values.")
        tags_df = tags_df[~missing_mask]

    num_cols = tags_df[required_cols].select_dtypes(include="number")
    invalid_ids = (tags_df[num_cols.columns] <= 0).any(axis=1)
    print(invalid_ids)
    if invalid_ids.any():
        print(f"Removed {invalid_ids.sum()} invalid IDs.")
        tags_df = tags_df[~invalid_ids]

    return tags_df

@task
def transform_tags_data(tags_df):
    tags_df = tags_df.explode("tags")
    tags_df = tags_df.reset_index(drop=True)
    tags_df = tags_df.rename(columns={"tags" : "tag"})

    normalized_tags_df = normalize_tags_dtypes(tags_df)

    cleared_tags_df = clean_and_validate_tags_data(normalized_tags_df)

    final_tags_df = pd.DataFrame()
    final_tags_df["tag_id"] =  cleared_tags_df["tag"].cat.codes + 1
    final_tags_df = final_tags_df.join(tags_df)

    return final_tags_df