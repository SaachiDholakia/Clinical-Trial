import pandas as pd


class DataValidationError(Exception):
    """Custom exception for data validation failures."""
    pass


def validate_dataframe(df: pd.DataFrame):

    # Required columns must exist
    required_columns = [
        "trial_id",
        "source",
        "registry_id",
        "title",
        "condition",
        "country",
        "ingestion_ts"
    ]

    for col in required_columns:
        if col not in df.columns:
            raise DataValidationError(f"Missing required column: {col}")

    #trial_id cannot be null
    if df["trial_id"].isnull().any():
        raise DataValidationError("Null trial_id values found")

    # trial_id must be unique
    if df["trial_id"].duplicated().any():
        raise DataValidationError("Duplicate trial_id values detected")

    # source cannot be null
    if df["source"].isnull().any():
        raise DataValidationError("Null source values detected")

    # ingestion_ts cannot be null
    if df["ingestion_ts"].isnull().any():
        raise DataValidationError("Null ingestion_ts detected")

    return True