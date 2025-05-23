import ast
import numpy as np
import pandas as pd

from utils import pull_from_gcs_csv, fetch_gcs_bucket, push_to_gcs_csv
from utils.metadata import (
    GCLOUD_BUCKET,
    GCLOUD_PROJECT,
    GCLOUD_PREPROCESSED_DIR,
    BENCHMARK_PREPROCESSED_DIR,
)


def format_database_dfs_to_nodes(
    df: pd.DataFrame,
    node_type: str,
    type_column: str,
    benchmark_names: dict = None,
    nan_columns: list = None,
) -> pd.DataFrame:
    """
    Format a dataframe based on node type and column mapping.

    Parameters:
        df (pd.DataFrame): Input dataframe to format.
        node_type (str): Type of node ('mine', 'processing', 'manufacturer').
        type_column (str): Column name to use for specific node types.
        benchmark_names (dict): Mapping of original column names to standardized names.
        nan_columns (list): List of columns to initialize with NaN values.

    Returns:
        pd.DataFrame: Formatted dataframe.
    """

    if benchmark_names is None:
        benchmark_names = {
            "node_id": "node_id",
            "Asset Name": "name",
            "owner1_short_clean": "company",
            "ISO3": "country",
        }
    if nan_columns is None:
        nan_columns = [
            "longitude",
            "latitude",
            "disruption_probability",
            "vulnerability",
            "mine_type",
            "process_type",
            "product_type",
        ]

    # Rename columns and set NaN values
    df_formatted = df[benchmark_names.keys()].rename(columns=benchmark_names)
    df_formatted[nan_columns] = np.nan

    # Add specific type column based on node type
    if node_type == "mine":
        df_formatted["mine_type"] = df[type_column]
    elif node_type == "processing":
        df_formatted["process_type"] = df[type_column]
    elif node_type == "manufacturer":
        df_formatted["product_type"] = df[type_column]
    elif node_type == "recycling":
        df_formatted["process_type"] = df[type_column]
    else:
        raise ValueError(
            "Invalid node_type. Choose from 'mine', 'processing', 'manufacturer' or 'recycling'."
        )

    # Add a 'parameters' column containing all other columns as a dictionary
    other_columns = [col for col in df.columns if col not in benchmark_names.keys()]
    df_formatted["parameters"] = df[other_columns].apply(
        lambda row: row.dropna().to_dict(), axis=1
    )

    return df_formatted


def format_partnership_dfs_to_edges(
    edge_df,
    df: pd.DataFrame,
    edge_type: str,
    benchmark_names: dict = None,
    nan_columns: list = None,
) -> pd.DataFrame:
    """
    Format a dataframe based on edge type and column mapping.

    Parameters:
        df (pd.DataFrame): Input dataframe to format.
        benchmark_names (dict): Mapping of original column names to standardized names.
        nan_columns (list): List of columns to initialize with NaN values.
    """

    if benchmark_names is None:
        benchmark_names = {
            "Materials": "flow_name",
            "source_ids": "source_ids",
            "target_ids": "target_ids",
            "Investment (USD)": "flow_volume",
        }
    if nan_columns is None:
        nan_columns = [
            "mode",
            "distance",
            "operators",
            "disruption_probability",
            "vulnerability",
        ]

    # Rename columns and set NaN values
    existing_cols = df.columns.intersection(benchmark_names.keys())
    df_formatted = df[existing_cols].rename(columns=benchmark_names)
    df_formatted[nan_columns] = np.nan

    starting_index = np.max(edge_df["edge_id"]) + 1 if not edge_df.empty else 0
    df_formatted["edge_id"] = df.index + starting_index
    df_formatted["type"] = "monetary"
    df_formatted["edge_type"] = edge_type

    # Add a 'parameters' column containing all other columns as a dictionary
    other_columns = [col for col in df.columns if col not in df_formatted.columns]
    df_formatted["parameters"] = df[other_columns].apply(
        lambda row: row.dropna().to_dict(), axis=1
    )

    edge_df = pd.concat((edge_df, df_formatted))

    return edge_df


def safe_eval(x):
    """
    Safely convert a string to a Python literal.
    If x is NaN or otherwise invalid, return an empty list.
    """
    if pd.isna(x):
        # If it's actual NaN (float('nan')), return empty list
        return []
    try:
        text = str(x).strip()
        # If it's something like 'nan', 'None', 'null', etc., treat it as empty
        if text.lower() in ["nan", "none", "null", ""]:
            return []
        return ast.literal_eval(text)
    except (SyntaxError, ValueError):
        # If it's not valid Python literal syntax, return []
        return []


if __name__ == "__main__":
    # Fetch the GCS bucket
    bucket = fetch_gcs_bucket(GCLOUD_PROJECT, GCLOUD_BUCKET)

    # Node file configurations
    node_files = [
        "lithium_mines",
        "lithium_processing",
        "cathode_manufacture",
        "batteries_manufacture",
        "recycling_data",
    ]
    node_types = ["mine", "processing", "manufacturer", "manufacturer", "recycling"]
    type_columns = ["Ore type", "Product 1", "Category", "Cell Format(s)", "Process"]

    # Initialize an empty DataFrame for nodes
    nodes = pd.DataFrame()

    # Process and concatenate nodes
    for file, node_type, type_column in zip(node_files, node_types, type_columns):
        df = pull_from_gcs_csv(
            bucket, GCLOUD_PREPROCESSED_DIR + BENCHMARK_PREPROCESSED_DIR + file + ".csv"
        )
        nodes = pd.concat(
            (nodes, format_database_dfs_to_nodes(df, node_type, type_column))
        )

    # Save nodes to GCS
    push_to_gcs_csv(
        nodes,
        bucket,
        GCLOUD_PREPROCESSED_DIR + BENCHMARK_PREPROCESSED_DIR + "benchmark_nodes.csv",
    )

    # Print result
    print("Node data formatting completed. Output DataFrame shape:", nodes.shape)

    # Edge file configurations
    edge_files = [
        "lithium_partnership",
        "cathode_partnership",
        "batteries_partnership",
        "recycling_partnership",
    ]
    edge_types = ["lithium", "cathode", "batteries", "recycling"]

    # Initialize an empty DataFrame for edges
    edges = pd.DataFrame()

    # Process and concatenate edges
    for file, edge_type in zip(edge_files, edge_types):
        df = pull_from_gcs_csv(
            bucket, GCLOUD_PREPROCESSED_DIR + BENCHMARK_PREPROCESSED_DIR + file + ".csv"
        )
        edges = format_partnership_dfs_to_edges(edges, df, edge_type)

    # Format edges
    edges = edges[
        [
            "edge_id",
            "source_ids",
            "target_ids",
            "flow_name",
            "flow_volume",
            "type",
            "mode",
            "distance",
            "operators",
            "disruption_probability",
            "vulnerability",
            "parameters",
        ]
    ]

    # Explode to get one line per match between source and target
    edges["source_ids"] = edges["source_ids"].astype(str)
    edges["source_ids"] = edges["source_ids"].apply(safe_eval)

    edges["target_ids"] = edges["target_ids"].astype(str)
    edges["target_ids"] = edges["target_ids"].apply(safe_eval)

    edges = edges.explode("source_ids", ignore_index=True)
    edges = edges.explode("target_ids", ignore_index=True)

    # Save edges to GCS
    push_to_gcs_csv(
        edges,
        bucket,
        GCLOUD_PREPROCESSED_DIR + BENCHMARK_PREPROCESSED_DIR + "benchmark_edges.csv",
    )

    print("Edge data formatting completed. Output DataFrame shape:", edges.shape)
