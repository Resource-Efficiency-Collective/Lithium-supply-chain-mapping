import json
import pandas as pd
from gcloud import storage


from utils.gcloud_utilities import (
    fetch_gcs_bucket,
    pull_from_gcs_excel,
    push_to_gcs_csv,
)
from utils.metadata import *
from preprocessing.config import BENCHMARK_HEADER_SIZE
from utils.companies import clean_company_name, shorten_company_name
from utils.countries import rename_countries

############ STEP 1: Load benchmark data from cloud ############


def pull_benchmark_sheet(
    bucket: storage.Bucket,  # Replace `Any` with the actual bucket type if available
    directory: str,
    file_name: str,
    sheet_name: str,
    skiprows: int = BENCHMARK_HEADER_SIZE,
    drop_columns: list = None,
) -> pd.DataFrame:
    """Fetch data from a GCS Excel file with error handling."""
    try:
        return pull_from_gcs_excel(
            bucket,
            directory + file_name,
            sheet_name=sheet_name,
            drop_columns=drop_columns,
            skiprows=skiprows,
        )
    except Exception as e:
        raise RuntimeError(f"Failed to fetch {file_name}, sheet: {sheet_name}") from e


def read_benchmark() -> tuple[
    pd.DataFrame,
    pd.DataFrame,
    pd.DataFrame,
    pd.DataFrame,
    pd.DataFrame,
    pd.DataFrame,
    pd.DataFrame,
    pd.DataFrame,
    pd.DataFrame,
    pd.DataFrame,
]:
    """utility function to fetch and process benchmark data
    from Google Cloud Storage (GCS)."""
    bucket = fetch_gcs_bucket(project_name=GCLOUD_PROJECT, bucket_name=GCLOUD_BUCKET)

    lithium_mines = pull_benchmark_sheet(
        bucket,
        BENCHMARK_LITHIUM_DIR,
        LITHIUM_FORECAST_FILE,
        "Li Supply",
        drop_columns=["Unnamed: 0"],
    )
    lithium_processing = pull_benchmark_sheet(
        bucket,
        BENCHMARK_LITHIUM_DIR,
        LITHIUM_FORECAST_FILE,
        "Li Chemical Supply",
        drop_columns=["Unnamed: 0"],
    )
    cathode_supply = pull_benchmark_sheet(
        bucket, BENCHMARK_CATHODE_DIR, CATHODE_FORECAST_FILE, "Cathode Supply"
    )
    cathode_supply_adjusted = pull_benchmark_sheet(
        bucket,
        BENCHMARK_CATHODE_DIR,
        CATHODE_FORECAST_FILE,
        "Cathode Supply (Adjusted)",
    )
    batteries_manufacture = pull_benchmark_sheet(
        bucket,
        BENCHMARK_LITHIUM_ION_BATTERIES_DIR,
        BATTERY_DATABASE_FILE,
        "Cell Supply",
    )
    recycling_data = pull_benchmark_sheet(
        bucket,
        BENCHMARK_RECYCLING_DIR,
        RECYCLING_DATABASE_FILE,
        "Battery Recycler's Database",
    )
    lithium_partnership = pull_benchmark_sheet(
        bucket,
        BENCHMARK_LITHIUM_DIR,
        LITHIUM_PARTNERSHIP_FILE,
        "Partnership Tracker Template",
    )
    cathode_partnership = pull_benchmark_sheet(
        bucket,
        BENCHMARK_CATHODE_DIR,
        CATHODE_PARTNERSHIP_FILE,
        "Cathode Partnership Tracker",
    )
    batteries_partnership = pull_benchmark_sheet(
        bucket,
        BENCHMARK_LITHIUM_ION_BATTERIES_DIR,
        BATTERIES_PARTNERSHIP_FILE,
        "Partnership Tracker",
    )
    recycling_partnership_2024 = pull_benchmark_sheet(
        bucket,
        BENCHMARK_RECYCLING_DIR,
        RECYCLING_PARTNERSHIP_FILE,
        "Partnership Tracker - 2024",
    )
    recycling_partnership_previous = pull_benchmark_sheet(
        bucket, BENCHMARK_RECYCLING_DIR, RECYCLING_PARTNERSHIP_FILE, "Previous years"
    )

    recycling_partnership = pd.concat(
        [recycling_partnership_2024, recycling_partnership_previous]
    ).reset_index(drop=True)

    return (
        lithium_mines,
        lithium_processing,
        cathode_supply,
        cathode_supply_adjusted,
        batteries_manufacture,
        recycling_data,
        lithium_partnership,
        cathode_partnership,
        batteries_partnership,
        recycling_partnership,
    )


def add_operator_for_duplicated_assets(data: pd.DataFrame) -> pd.DataFrame:
    """when identical asset names are used by different companies so we add the operator name between parenthesis
    for duplicated indexes"""
    dupli_index = data[data["Asset Name"].duplicated()].index
    data.loc[dupli_index, "Asset Name"] = (
        data.loc[dupli_index, "Asset Name"]
        + " ("
        + data.loc[dupli_index, "Operator"]
        + ")"
    )
    # Fix nan asset name values:
    nan_ind = data[data["Asset Name"].isna()].index
    data.loc[nan_ind, "Asset Name"] = "(" + data.loc[nan_ind, "Operator"] + ")"

    return data


############ STEP 2: Add unique node_id numbers to files containing nodes ############


def add_nodes_to_dict(
    nodes_to_add: list, node_to_id: dict, id_to_node: dict
) -> tuple[dict, dict]:
    """
    Add new nodes to the node-to-ID and ID-to-node dictionaries.
    """
    existing_nodes = set(node_to_id)
    new_nodes = set(nodes_to_add) - existing_nodes

    start_id = len(node_to_id)
    node_to_id.update({name: start_id + i for i, name in enumerate(new_nodes)})
    id_to_node.update({start_id + i: name for i, name in enumerate(new_nodes)})

    return node_to_id, id_to_node


def add_nodes_to_df_and_dict(
    dataframes: list, node_to_id=None, id_to_node=None
) -> tuple[list, dict, dict]:
    """
    Preprocess a list of DataFrames by adding node IDs based on the 'Asset Name' column.

    Parameters:
    - dataframes (list): List of pandas DataFrames to process.
    - node_to_id (dict): Existing node-to-ID dictionary.
    - id_to_node (dict): Existing ID-to-node dictionary.

    Returns:
    - dataframes (list): List of processed DataFrames with 'node_id' column as the index.
    - node_to_id (dict): Updated node-to-ID dictionary.
    - id_to_node (dict): Updated ID-to-node dictionary.
    """

    if id_to_node is None:
        id_to_node = {}
    if node_to_id is None:
        node_to_id = {}
    processed_dataframes = []

    for df in dataframes:
        # Extract unique names from the 'Asset Name' column
        names_to_add = list(df["Asset Name"].dropna())
        node_to_id, id_to_node = add_nodes_to_dict(names_to_add, node_to_id, id_to_node)

        # Map 'Asset Name' to node IDs and update the DataFrame
        df["node_id"] = df["Asset Name"].replace(node_to_id).fillna(-1).astype(int)
        # df = df.reset_index().drop("index", axis=1).set_index("node_id")

        # Add to the list of processed DataFrames
        processed_dataframes.append(df)

    return processed_dataframes, node_to_id, id_to_node


############ STEP 3: For each partnership, add the nodes it relates to ############


def load_json_to_dict(filename: str) -> dict:
    """Load a JSON file into a Python dictionary."""
    try:
        with open(filename, "r") as file:
            return json.load(file)
    except (FileNotFoundError, json.JSONDecodeError) as e:
        print(f"Error loading JSON: {e}")
        return {}


def associate_node_id(
    df: pd.DataFrame,
    matches_dict: dict,
    node_to_id_dict: dict,
    asset_col: str,
    id_col_name="source_ids",
) -> pd.DataFrame:
    """Associate the node_ids to the nodes each partnership has been associated with."""
    df[id_col_name] = (
        df[asset_col]
        .map(matches_dict)
        .apply(
            lambda x: (
                [
                    node_to_id_dict.get(
                        item,
                    )
                    for item in x
                ]
                if isinstance(x, list)
                else x
            )
        )
    )

    return df


############ STEP 4: Get all possible company names and rename them in a common format ############


def get_all_company_names(datasets: list) -> list:
    def extract_partners(dataframe, columns):
        """Helper function to extract unique partners from specified columns."""
        return set(dataframe[columns].stack())

    # Define columns for each dataset
    partnership_columns = [
        "Partner 1",
        "Partner 2",
        "Partner 3",
        "Partner 4",
        "Partner 5",
    ]
    ownership_columns = [
        "Operator",
        "Primary Project Owner",
        "Secondary owner name",
        "Third Owner",
        "Fourth Owner",
        "Fifth Owner",
    ]

    # Create a list of all sets
    sets = [
        extract_partners(lithium_partnership, ["Buyer 1", "Seller 1"]),
        extract_partners(cathode_partnership, partnership_columns),
        extract_partners(batteries_partnership, partnership_columns),
        extract_partners(recycling_partnership, partnership_columns),
        extract_partners(lithium_mines, ownership_columns[:4]),
        extract_partners(lithium_processing, ownership_columns[:4]),
        extract_partners(cathode_supply, ownership_columns[:4]),
        extract_partners(cathode_supply_adjusted, ownership_columns[:4]),
        extract_partners(batteries_manufacture, ownership_columns),
        extract_partners(recycling_data, ["Operator"]),
    ]

    # Compute the union of all sets in the list
    all_companies = set().union(*sets)

    # Convert to list and remove any undesired elements (e.g., blank or null entries)
    all_companies = list(filter(None, all_companies))

    return all_companies


############ STEP 5: Add short clean company names to datasets ############


def add_short_clean_company_names(dataset, columns, target_columns) -> None:
    """Add short clean company names to the dataset."""
    dataset[target_columns] = (
        dataset[columns].replace(dict_clean_companies).replace(dict_short_companies)
    )


############ STEP 6: Harmonize country names ############


def harmonize_partner_countries(dataset, num_partners, prefix):
    for i in range(1, num_partners + 1):
        country_col = f"{prefix} {i} Country"
        iso3_col = f"{prefix} {i} ISO3"
        dataset[iso3_col] = rename_countries(dataset, country_col)[country_col]


if __name__ == "__main__":
    ############ STEP 1: Load benchmark data from cloud ###########
    benchmark_data_cloud = read_benchmark()
    benchmark_data = [data.copy() for data in benchmark_data_cloud]

    (
        lithium_mines,
        lithium_processing,
        cathode_supply,
        cathode_supply_adjusted,
        batteries_manufacture,
        recycling_data,
        lithium_partnership,
        cathode_partnership,
        batteries_partnership,
        recycling_partnership,
    ) = benchmark_data

    recycling_data = add_operator_for_duplicated_assets(recycling_data)

    # Standardise column names across datasets
    rename_mappings = {
        "Secondary Owner": "Secondary owner name",
        "Secondary Owner ": "Secondary owner name",
    }
    cathode_supply.rename(columns=rename_mappings, inplace=True)
    cathode_supply_adjusted.rename(columns=rename_mappings, inplace=True)

    print("Benchmark data loaded successfully.")

    ############ STEP 2: Add unique node_id numbers to files containing nodes ############

    node_to_id_dict, id_to_node_dict = {}, {}

    (
        (
            lithium_mines,
            lithium_processing,
            cathode_manufacture,
            batteries_manufacture,
            recycling_data,
        ),
        node_to_id_dict,
        id_to_node_dict,
    ) = add_nodes_to_df_and_dict(
        [
            lithium_mines,
            lithium_processing,
            cathode_supply,
            batteries_manufacture,
            recycling_data,
        ],
        node_to_id_dict,
        id_to_node_dict,
    )

    print("Node IDs added successfully.")

    ############ STEP 3: For each partnership, add the nodes it relates to / is bought from ############

    partnership_matches_dict = load_json_to_dict(
        "preprocessing/matching_jsons/partnership_matching.json"
    )

    # Map source of partnership to nodes
    lithium_partnership["Mine Asset match"] = lithium_partnership["Asset Name"].map(
        partnership_matches_dict
    )
    cathode_partnership["Facility Asset match"] = cathode_partnership["Partner 1"].map(
        partnership_matches_dict
    )
    batteries_partnership["Facility Asset match"] = batteries_partnership[
        "Partner 1"
    ].map(partnership_matches_dict)
    recycling_partnership["Facility Asset match"] = recycling_partnership[
        "Partner 1"
    ].map(partnership_matches_dict)

    # Match source_node_ids to nodes
    lithium_partnership = associate_node_id(
        lithium_partnership,
        partnership_matches_dict,
        node_to_id_dict,
        "Asset Name",
        "source_ids",
    )
    cathode_partnership = associate_node_id(
        cathode_partnership,
        partnership_matches_dict,
        node_to_id_dict,
        "Partner 1",
        "source_ids",
    )
    batteries_partnership = associate_node_id(
        batteries_partnership,
        partnership_matches_dict,
        node_to_id_dict,
        "Partner 1",
        "source_ids",
    )
    recycling_partnership = associate_node_id(
        recycling_partnership,
        partnership_matches_dict,
        node_to_id_dict,
        "Partner 1",
        "source_ids",
    )

    # Map target of partnership to nodes
    lithium_partnership["Buyer match"] = lithium_partnership["Buyer 1"].map(
        partnership_matches_dict
    )
    cathode_partnership["Buyer match"] = cathode_partnership["Partner 2"].map(
        partnership_matches_dict
    )
    batteries_partnership["Buyer match"] = batteries_partnership["Partner 2"].map(
        partnership_matches_dict
    )
    recycling_partnership["Buyer match"] = recycling_partnership["Partner 2"].map(
        partnership_matches_dict
    )

    # Match target_node_ids to nodes
    lithium_partnership = associate_node_id(
        lithium_partnership,
        partnership_matches_dict,
        node_to_id_dict,
        "Buyer 1",
        "target_ids",
    )
    cathode_partnership = associate_node_id(
        cathode_partnership,
        partnership_matches_dict,
        node_to_id_dict,
        "Partner 2",
        "target_ids",
    )
    batteries_partnership = associate_node_id(
        batteries_partnership,
        partnership_matches_dict,
        node_to_id_dict,
        "Partner 2",
        "target_ids",
    )
    recycling_partnership = associate_node_id(
        recycling_partnership,
        partnership_matches_dict,
        node_to_id_dict,
        "Partner 2",
        "target_ids",
    )

    print("Nodes added to partnerships succesfully.")

    ############ STEP 4: Get all possible company names and rename them in a common format ############

    datasets = [
        lithium_mines,
        lithium_processing,
        cathode_manufacture,
        # cathode_supply,
        # cathode_supply_adjusted,
        batteries_manufacture,
        recycling_data,
        lithium_partnership,
        cathode_partnership,
        batteries_partnership,
        recycling_partnership,
    ]

    all_companies = get_all_company_names(datasets)
    all_companies_clean = clean_company_name(all_companies)
    all_companies_short = shorten_company_name(all_companies_clean)

    # Print the counts of unique companies in each dataset
    print(f"Total unique companies (all): {len(set(all_companies))}")  # 2596
    print(f"Total unique companies (cleaned): {len(set(all_companies_clean))}")  # 2466
    print(
        f"Total unique companies (short list): {len(set(all_companies_short))}"
    )  # 2315

    dict_clean_companies = dict(zip(all_companies, all_companies_clean))
    dict_short_companies = dict(zip(all_companies_clean, all_companies_short))

    print("Company names cleaned and shortened successfully.")

    ############ STEP 5: Add short clean company names to datasets ############

    # Convert node datasets
    node_datasets = [
        lithium_mines,
        lithium_processing,
        cathode_manufacture,
        # cathode_supply,
        # cathode_supply_adjusted,
        batteries_manufacture,
        recycling_data,
    ]
    node_columns = [
        "Operator",
        "Primary Project Owner",
        "Secondary owner name",
        "Third Owner",
        "Fourth Owner",
        "Fifth Owner",
    ]

    used_columns = [
        node_columns[:4],
        node_columns[:4],
        node_columns[:4],
        # node_columns[:4],
        node_columns,
        node_columns[:1],
    ]
    target_columns_list = [
        ["operator_short_clean"]
        + [f"owner{i}_short_clean" for i in range(1, len(columns))]
        for columns in used_columns
    ]

    for dataset, columns, targets in zip(
        node_datasets, used_columns, target_columns_list
    ):
        add_short_clean_company_names(dataset, columns, targets)

    recycling_data["owner1_short_clean"] = recycling_data["Operator"]

    # Convert partnership datasets
    partnership_datasets = [
        lithium_partnership,
        cathode_partnership,
        batteries_partnership,
        recycling_partnership,
    ]
    partnership_columns = [
        "Partner 1",
        "Partner 2",
        "Partner 3",
        "Partner 4",
        "Partner 5",
    ]

    used_columns = [
        ["Buyer 1", "Seller 1"],
        partnership_columns,
        partnership_columns,
        partnership_columns,
    ]
    target_columns_list = [
        [item.lower().replace(" ", "") for item in column_names]
        for column_names in used_columns
    ]

    for dataset, columns, targets in zip(
        partnership_datasets, used_columns, target_columns_list
    ):
        add_short_clean_company_names(dataset, columns, targets)

    print("Short clean company names added successfully.")

    ############ STEP 6: Harmonize country names ############

    datasets = [
        lithium_mines,
        lithium_processing,
        cathode_manufacture,
        # cathode_supply,
        # cathode_supply_adjusted,
        batteries_manufacture,
        recycling_data,
    ]

    for dataset in datasets:
        dataset["ISO3"] = rename_countries(dataset, "Country")["Country"]

    lithium_partnership = lithium_partnership.assign(
        **{
            "Asset ISO3": rename_countries(lithium_partnership, "Asset Country")[
                "Asset Country"
            ],
            "Buyer 1 ISO3": rename_countries(lithium_partnership, "Buyer 1 Country")[
                "Buyer 1 Country"
            ],
        }
    )

    # Harmonize ISO3 country codes for cathode_partnership and batteries_partnership
    harmonize_partner_countries(cathode_partnership, 5, "Partner")
    harmonize_partner_countries(batteries_partnership, 5, "Partner")

    ############ STEP 7: Save the processed data to cloud ############

    node_datasets = {
        "lithium_mines": lithium_mines,
        "lithium_processing": lithium_processing,
        "cathode_manufacture": cathode_manufacture,
        # "cathode_supply": cathode_supply,
        # "cathode_supply_adjusted": cathode_supply_adjusted,
        "batteries_manufacture": batteries_manufacture,
        "recycling_data": recycling_data,
        "lithium_partnership": lithium_partnership,
        "cathode_partnership": cathode_partnership,
        "batteries_partnership": batteries_partnership,
        "recycling_partnership": recycling_partnership,
    }

    bucket = fetch_gcs_bucket(GCLOUD_PROJECT, GCLOUD_BUCKET)

    for name, dataset in node_datasets.items():
        push_to_gcs_csv(
            dataset, bucket, f"{GCLOUD_PREPROCESSED_DIR}benchmark/{name}.csv"
        )

############ STEP 6: Find matching node_ids for partnerships ############
# for lithium_partnerships the matches have been defined directly with the "Asset Name" in step3

columns_to_combine_3owners = [
    "operator_short_clean",
    "owner1_short_clean",
    "owner2_short_clean",
    "owner3_short_clean",
]
# lithium_processing["all_related_companies"] = lithium_processing[
#     columns_to_combine_3owners
# ].apply(lambda row: [x for x in row if not pd.isna(x)], axis=1)
# cathode_supply["all_related_companies"] = cathode_supply[
#     columns_to_combine_3owners
# ].apply(lambda row: [x for x in row if not pd.isna(x)], axis=1)
# batteries_manufacture["all_related_companies"] = batteries_manufacture[
#     columns_to_combine_3owners
# ].apply(lambda row: [x for x in row if not pd.isna(x)], axis=1)
# recycling_data["all_related_companies"] = recycling_data[["Operator"]].apply(
#     lambda row: [x for x in row if not pd.isna(x)], axis=1
# )
#
# columns_to_combine = [
#     "operator_short_clean",
#     "owner1_short_clean",
#     "owner2_short_clean",
#     "owner3_short_clean",
#     "owner4_short_clean",
#     "owner5_short_clean",
# ]
# cathode_partnership["all_related_companies"] = cathode_partnership[
#     partners_short_clean
# ].apply(lambda row: [x for x in row if not pd.isna(x)], axis=1)
# batteries_partnership["all_related_companies"] = batteries_partnership[
#     partners_short_clean
# ].apply(lambda row: [x for x in row if not pd.isna(x)], axis=1)
# recycling_partnership["all_related_companies"] = recycling_partnership[
#     partners_short_clean
# ].apply(lambda row: [x for x in row if not pd.isna(x)], axis=1)
#
#
# # Function to find matching node_ids
# def find_matching_node_ids(row, supply_data):
#     # Initialize an empty set to store unique node_ids
#     matching_node_ids = set()
#
#     # Iterate over each company in the current row's 'all_related_companies'
#     for company in row:
#         # Find node_ids in cathode_supply where 'all_related_companies' contains the company
#         matches = supply_data[
#             supply_data["all_related_companies"].apply(lambda x: company in x)
#         ]["node_id"]
#         # Add these node_ids to the set
#         matching_node_ids.update(matches)
#
#     # Return the set of matching node_ids as a sorted list
#     return sorted(matching_node_ids)
#
#
# # Apply the function to each row in cathode_partnership
# cathode_partnership["potential_cathode_supply_facility_match"] = cathode_partnership[
#     "all_related_companies"
# ].apply(lambda companies: find_matching_node_ids(companies, cathode_supply))
# cathode_partnership[
#     "potential_lithium_processing_facility_match"
# ] = cathode_partnership["all_related_companies"].apply(
#     lambda companies: find_matching_node_ids(companies, lithium_processing)
# )
# cathode_partnership["potential_batteries_facility_match"] = cathode_partnership[
#     "all_related_companies"
# ].apply(lambda companies: find_matching_node_ids(companies, batteries_manufacture))
