import ast
import pandas as pd
from utils.metadata import *
from utils.gcloud_utilities import *


def safe_divide(a, b):
    return a / b if b != 0 else float("inf")


def expand_parameters_col_and_format(df, year, parameters_col_name="parameters"):
    """
    Expand the parameters column in the nodes DataFrame and format the resulting DataFrame.
    """
    df[parameters_col_name] = df[parameters_col_name].apply(ast.literal_eval)
    dict_df = pd.json_normalize(df[parameters_col_name])
    df = df.drop(columns=[parameters_col_name]).join(dict_df)
    df[year] = df[year].replace("-", 0).astype(float)
    return df


def import_operating_nodes(year: str) -> [storage.Bucket, pd.DataFrame]:
    """Import operating nodes from GCS and format the DataFrame for the year in question."""
    # Fetch GCS bucket
    bucket = fetch_gcs_bucket(project_name=GCLOUD_PROJECT, bucket_name=GCLOUD_BUCKET)

    # Load node and edge data from GCS
    nodes = pull_from_gcs_csv(
        bucket,
        GCLOUD_PREPROCESSED_DIR + BENCHMARK_PREPROCESSED_DIR + BENCHMARK_NODES_FILE,
    )
    nodes = expand_parameters_col_and_format(nodes, year)

    # Only keep operational and producing nodes for edge creation
    nodes = nodes[nodes["Status"] == "Operating"]
    nodes = nodes[nodes[year] > 0]

    return bucket, nodes


def allocate_volume_to_targets(
    edges: pd.DataFrame,
    source: int,
    targets: pd.DataFrame,
    remaining_target_df: pd.DataFrame,
    ratio: float,
    year: str,
    edge_type: str,
    edge_properties: dict = None,
) -> [pd.DataFrame, pd.DataFrame]:
    """
    Allocate volume from a source to a list of targets based on a ratio of available/target_total.
    """
    for i, row in targets.iterrows():
        target = row["node_id_y"]
        volume = remaining_target_df.loc[target, year] * ratio

        # print({k: row[v] for k, v in edge_properties.items()})

        edges = pd.concat(
            (
                edges,
                pd.DataFrame(
                    {
                        "source": [source],
                        "target": [target],
                        year + "_volume": [volume],
                        "edge_type": [row[edge_type]],
                        "properties": [{}]
                        if edge_properties is None
                        else [{k: row[v] for k, v in edge_properties.items()}],
                    }
                ),
            )
        )

        remaining_target_df.loc[target, year] = (
            remaining_target_df.loc[target, year] - volume
        )

    return edges, remaining_target_df


def create_edges_from_facility_matches(
    edges: pd.DataFrame,
    remaining_source_df: pd.DataFrame,
    remaining_target_df: pd.DataFrame,
    match_df: pd.DataFrame,
    input_cols: list,
    year: str,
    edge_type: str,
    edge_properties: dict = None,
) -> [pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """
    Create edges between source and target facilities based on a match dataframe.
    """

    for source in match_df["node_id_x"].unique():

        targets = match_df[match_df["node_id_x"] == source]

        available = remaining_source_df.loc[source, year]
        target_total = remaining_target_df.loc[list(targets["node_id_y"]), year].sum()
        ratio = min(1, safe_divide(available, target_total))

        edges, remaining_target_df = allocate_volume_to_targets(
            edges,
            source,
            targets,
            remaining_target_df,
            ratio,
            year,
            edge_type,
            edge_properties,
        )

        remaining_source_df.loc[source, year] = max(
            0, remaining_source_df.loc[source, year] - target_total
        )

    return edges, remaining_source_df, remaining_target_df


def even_source_target_allocation(
    edges: pd.DataFrame,
    remaining_source_df: pd.DataFrame,
    remaining_target_df: pd.DataFrame,
    match_df: pd.DataFrame,
    input_cols: list,
    year: str,
    edge_type: str,
    edge_properties: dict = None,
) -> [pd.DataFrame, pd.DataFrame]:
    """
    Allocate volume from source to target based on a ratio of available/target_total.
    """
    for row in match_df[input_cols].drop_duplicates().iterrows():
        # Find all the edges for the current features and calculate the available source and target materials
        property_matches = match_df[(match_df[input_cols] == row[1]).all(axis=1)]
        available_total = remaining_source_df.loc[
            list(property_matches["node_id_x"].unique()), year
        ].sum()
        target_total = remaining_target_df.loc[
            list(property_matches["node_id_y"].unique()), year
        ].sum()

        # Calculate the ratios of source and targets used for this set of edges
        source_utilisation_ratio = min(1, safe_divide(target_total, available_total))
        target_completion_ratio = min(1, safe_divide(available_total, target_total))

        # Create new edges based on the target completion ratio going to each target
        new_edges = property_matches.copy()
        new_edges[year + "_x"] = remaining_source_df.loc[
            list(property_matches["node_id_x"]), year
        ].values
        new_edges[year + "_y"] = remaining_target_df.loc[
            list(property_matches["node_id_y"]), year
        ].values
        new_edges[year] = (
            new_edges[year + "_x"] * source_utilisation_ratio
        ) * safe_divide(new_edges[year + "_y"], target_total)

        edges = pd.concat(
            (
                edges,
                pd.DataFrame(
                    {
                        "source": new_edges["node_id_x"],
                        "target": new_edges["node_id_y"],
                        year + "_volume": new_edges[year],
                        "edge_type": new_edges[edge_type],
                        "properties": [{}] * len(new_edges)
                        if edge_properties is None
                        else [
                            {k: edge_row[v] for k, v in edge_properties.items()}
                            for edge_row in new_edges.iloc
                        ],
                    }
                ),
            )
        )

        # Subtract the total edge volume from the remaining source materials
        remaining_source_df.loc[list(property_matches["node_id_x"].unique()), year] = (
            remaining_source_df.loc[list(property_matches["node_id_x"].unique()), year]
            - remaining_source_df.loc[
                list(property_matches["node_id_x"].unique()), year
            ]
            * source_utilisation_ratio
        )

        remaining_target_df.loc[list(property_matches["node_id_y"].unique()), year] = (
            remaining_target_df.loc[list(property_matches["node_id_y"].unique()), year]
            - remaining_target_df.loc[
                list(property_matches["node_id_y"].unique()), year
            ]
            * target_completion_ratio
        )

    return edges, remaining_source_df, remaining_target_df


def create_edge_combinations(
    source_nodes: pd.DataFrame,
    target_nodes: pd.DataFrame,
    remaining_source: pd.DataFrame,
    remaining_target: pd.DataFrame,
    input_cols: list,
    output_cols: list,
    edge_type: str,
    year: str,
    edges=None,
    verbose: bool = True,
    allocation_method="even",
    edge_properties: dict = None,
) -> [pd.DataFrame, pd.DataFrame, pd.DataFrame]:

    """Create edges between source and target nodes based on the links between input and output columns"""

    if edges is None:
        edges = pd.DataFrame()

    # Allocation method either creates an edge to all possible matches within defined criteria and allocates material evenly, or creates edges based on the first come first serve basis of the matches for each source node
    if allocation_method == "even":
        allocation_function = even_source_target_allocation
    elif allocation_method == "first come first served":
        allocation_function = create_edges_from_facility_matches
    else:
        raise ValueError("Allocation method not recognised.")

    if verbose:
        print(
            "Total source volume: "
            + str(remaining_source[year].sum())
            + ", Total target volume: "
            + str(remaining_target[year].sum())
        )
        unmatched = remaining_target[year].sum()

    company_country_match = source_nodes.merge(
        target_nodes, left_on=input_cols, right_on=output_cols, how="inner"
    )

    input_cols_allocation = [
        edge_type if col in edge_type else col for col in input_cols
    ]

    edges, remaining_source, remaining_target = allocation_function(
        edges,
        remaining_source,
        remaining_target,
        company_country_match,
        input_cols_allocation,
        year,
        edge_type,
        edge_properties=edge_properties,
    )

    if verbose:
        print(
            "Company & country match: "
            + str(unmatched - remaining_target[year].sum())
            + ", Remaining source volume: "
            + str(remaining_source[year].sum())
            + ", Remaining target volume: "
            + str(remaining_target[year].sum())
        )
        unmatched = remaining_target[year].sum()

    country_match = source_nodes.merge(
        target_nodes, left_on=input_cols[:-1], right_on=output_cols[:-1], how="inner"
    )

    edges, remaining_source, remaining_target = allocation_function(
        edges,
        remaining_source,
        remaining_target,
        country_match,
        input_cols_allocation[:-1],
        year,
        edge_type,
        edge_properties=edge_properties,
    )

    if verbose:
        print(
            "Just country match: "
            + str(unmatched - remaining_target[year].sum())
            + ", Remaining source volume: "
            + str(remaining_source[year].sum())
            + ", Remaining target volume: "
            + str(remaining_target[year].sum())
        )
        unmatched = remaining_target[year].sum()

    company_match = source_nodes.merge(
        target_nodes,
        left_on=[input_cols[0], input_cols[-1]],
        right_on=[output_cols[0], output_cols[-1]],
        how="inner",
    )

    edges, remaining_source, remaining_target = allocation_function(
        edges,
        remaining_source,
        remaining_target,
        company_match,
        [input_cols_allocation[0], input_cols_allocation[-1]],
        year,
        edge_type,
        edge_properties=edge_properties,
    )

    if verbose:
        print(
            "Just company match: "
            + str(unmatched - remaining_target[year].sum())
            + ", Remaining source volume: "
            + str(remaining_source[year].sum())
            + ", Remaining target volume: "
            + str(remaining_target[year].sum())
        )
        unmatched = remaining_target[year].sum()

    type_match = source_nodes.merge(
        target_nodes, left_on=[input_cols[0]], right_on=[output_cols[0]], how="inner"
    )

    edges, remaining_source, remaining_target = allocation_function(
        edges,
        remaining_source,
        remaining_target,
        type_match,
        [input_cols_allocation[0]],
        year,
        edge_type,
        edge_properties=edge_properties,
    )

    if verbose:
        print(
            "Just type match: "
            + str(unmatched - remaining_target[year].sum())
            + ", Remaining source volume: "
            + str(remaining_source[year].sum())
            + ", Remaining target volume: "
            + str(remaining_target[year].sum())
        )
        print("Final unmatched target volume: " + str(remaining_target[year].sum()))

    return edges, remaining_source, remaining_target


def save_nonzero_edges(
    edges: pd.DataFrame, year: str, bucket: storage.Bucket, edge_filename: str
):
    """
    Save edges to GCS where the volume is greater than 0.
    """
    resulting_edges = edges[edges[year + "_volume"] > 0]
    push_to_gcs_csv(
        resulting_edges,
        bucket,
        GCLOUD_PREPROCESSED_DIR
        + BENCHMARK_PREPROCESSED_DIR
        + BENCHMARK_EDGES_DIR
        + edge_filename,
    )


def match_use_types(
    edges,
    source_nodes,
    target_nodes,
    remaining_source,
    remaining_target,
    input_cols,
    output_cols,
    year,
    edge_type,
    output_type,
    edge_properties=None,
):

    unmatched = remaining_target[year].sum()

    match_df = source_nodes.merge(
        target_nodes, left_on=input_cols, right_on=output_cols, how="inner"
    )

    edges, remaining_source, remaining_target = even_source_target_allocation(
        edges,
        remaining_source,
        remaining_target,
        match_df,
        input_cols,
        year,
        edge_type,
        edge_properties=edge_properties,
    )

    print(
        output_type
        + " match: "
        + str(unmatched - remaining_target[year].sum())
        + ", Remaining source volume: "
        + str(remaining_source[year].sum())
        + ", Remaining target volume: "
        + str(remaining_target[year].sum())
    )

    return edges, remaining_source, remaining_target


def run_sequence_of_end_use_mixes(
    edges,
    source_nodes,
    target_nodes,
    remaining_source,
    remaining_target,
    input_cols,
    output_cols,
    year,
    edge_type,
    mixed_output_1=None,
    mixed_output_2=None,
    mixed_output_3=None,
):

    if mixed_output_1 is None:
        mixed_output_1 = {"EV": "EV, Portable", "Portable": "EV, Portable"}
    if mixed_output_2 is None:
        mixed_output_2 = {"EV": "EV, ESS", "ESS": "EV, ESS"}
    if mixed_output_3 is None:
        mixed_output_3 = {"Portable": "ESS, Portable", "ESS": "ESS, Portable"}

    edges, remaining_source, remaining_target = match_use_types(
        edges,
        source_nodes,
        target_nodes,
        remaining_source,
        remaining_target,
        input_cols,
        output_cols,
        year,
        edge_type,
        "Single use",
    )

    target_nodes["product_type"] = target_nodes["product_type_orig"].replace(
        mixed_output_1
    )

    edges, remaining_source, remaining_target = match_use_types(
        edges,
        source_nodes,
        target_nodes,
        remaining_source,
        remaining_target,
        input_cols,
        output_cols,
        year,
        edge_type,
        "EV, Portable mixed output",
    )

    target_nodes["product_type"] = target_nodes["product_type_orig"].replace(
        mixed_output_2
    )

    edges, remaining_source, remaining_target = match_use_types(
        edges,
        source_nodes,
        target_nodes,
        remaining_source,
        remaining_target,
        input_cols,
        output_cols,
        year,
        edge_type,
        "EV, ESS mixed output",
    )

    target_nodes["product_type"] = target_nodes["product_type_orig"].replace(
        mixed_output_3
    )

    edges, remaining_source, remaining_target = match_use_types(
        edges,
        source_nodes,
        target_nodes,
        remaining_source,
        remaining_target,
        input_cols,
        output_cols,
        year,
        edge_type,
        "ESS, Portable mixed output",
    )

    print("Final unmatched target volume: " + str(remaining_target[year].sum()))

    return edges, remaining_source, remaining_target


def concatenate_edge_files(
    edge_files: list, filename: str = "benchmark_combined_edges.csv"
):

    bucket = fetch_gcs_bucket(GCLOUD_PROJECT, GCLOUD_BUCKET)

    edges = pd.DataFrame()

    for edge_file in edge_files:
        file_edges = pull_from_gcs_csv(
            bucket,
            GCLOUD_PREPROCESSED_DIR
            + BENCHMARK_PREPROCESSED_DIR
            + BENCHMARK_EDGES_DIR
            + edge_file,
        )
        edges = pd.concat((edges, file_edges))

    push_to_gcs_csv(
        edges,
        bucket,
        GCLOUD_PREPROCESSED_DIR
        + BENCHMARK_PREPROCESSED_DIR
        + BENCHMARK_EDGES_DIR
        + filename,
    )
