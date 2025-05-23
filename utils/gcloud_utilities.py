"""Module for defining postprocessing that interact with Google Cloud Storage."""

import io
import pandas as pd
from google.cloud import storage
from loguru import logger


def fetch_gcs_bucket(project_name: str, bucket_name: str) -> storage.Bucket:
    """
    Initialize Google Cloud Storage client.
    Need to set 'GOOGLE_APPLICATION_CREDENTIALS' as the path to your gcloud
    credentials in a .env file in the root directory of the repo.
    """

    logger.info(f"Fetching GCS bucket: {bucket_name} in project: {project_name}'")

    storage_client = storage.Client(project=project_name)
    bucket = storage_client.get_bucket(bucket_name)

    return bucket


def pull_from_gcs_csv(bucket: storage.Bucket, filepath: str) -> pd.DataFrame:
    """Load data from a GCS file into a DataFrame."""

    logger.info(f"Pulling data from {filepath} in bucket {bucket.name}")

    blob = bucket.blob(filepath)
    data = blob.download_as_text()
    csv_data = io.StringIO(data)
    df = pd.read_csv(csv_data)

    return df


def pull_from_gcs_excel(
    bucket: storage.Bucket,
    filepath: str,
    sheet_name: str = None,
    drop_columns: list = None,
    skiprows: int = None,
    header: int = None,
) -> pd.DataFrame:
    """Load data from a Excel file on GCS to DataFrame."""

    logger.info(f"Pulling data from {filepath} in bucket {bucket.name}")

    blob = bucket.blob(filepath)
    data = blob.download_as_bytes()  # Download as binary
    excel_data = io.BytesIO(data)  # Convert binary to BytesIO for Excel

    df = pd.read_excel(excel_data, sheet_name=sheet_name, skiprows=skiprows)
    if drop_columns:
        df = df.drop(columns=drop_columns)

    return df


def push_to_gcs_csv(
    df: pd.DataFrame, bucket: storage.Bucket, filepath: str, index: bool = False
):
    """Save a DataFrame to a CSV file in GCS."""

    logger.info(f"Saving data to {filepath} in bucket {bucket.name}")

    csv_data = df.to_csv(index=index)
    blob = bucket.blob(filepath)
    blob.upload_from_string(csv_data)
