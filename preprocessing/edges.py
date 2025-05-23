import os
import sys

project_root = os.path.abspath(os.path.join(os.getcwd(), ".."))
if project_root not in sys.path:
    sys.path.insert(0, project_root)
from utils.preprocessing_utilities import *
from utils.gcloud_utilities import *
from utils.metadata import *


def battery_material_edges():
    """
    Create edges between battery and cathode nodes based on the percentage of each feedstock
    """

    # Variables
    year = "2023"
    edge_filename = "battery_material_edges.csv"
    cathode_types = [
        "NCM mid nickel",
        "LFP",
        "4V Ni or Mn based",
        "NCA",
        "NCM high nickel",
        "LCO",
        "NCM low nickel",
        "5V Mn based",
        "Under Review",
        "Other",
    ]
    battery_types = [
        "Cylindrical",
        "Pouch",
        "Cylindrical, Pouch",
        "Pouch, Prismatic",
        "Prismatic",
        "Cylindrical, Prismatic",
        "Cylindrical, Pouch, Prismatic",
    ]
    input_cols = ["product_type", "country", "company"]
    output_cols = ["Feedstock", "country", "company"]
    edge_type = "product_type_x"
    allocation_method = "even"  # 'even' or 'first come first serve'

    ## Load data
    bucket, nodes = import_operating_nodes(year)

    # Split out cathodes and batteries
    cathodes = nodes[[i in cathode_types for i in nodes["product_type"]]]
    batteries = nodes[[i in battery_types for i in nodes["product_type"]]]

    ## Filter cathode nodes
    cathodes_used = cathodes[
        ["node_id", "country", "operator_short_clean", "product_type", year]
    ].rename(columns={"operator_short_clean": "company"})

    # Add node id prefix based on cathode type due to multiple cathode types from some facilities
    prefix_values = [str(i) for i in range(10, 56, 5)]  # Define prefix order
    prefix_map = dict(zip(cathode_types, prefix_values))  # Create mapping
    cathodes_used["node_id"] = (
        cathodes_used["product_type"].map(prefix_map)
        + cathodes_used["node_id"].astype(str)
    ).astype(int)

    ## Filter battery nodes
    batteries_used = batteries[
        ["node_id", "country", "company", "product_type", year] + cathode_types[:-2]
    ]

    # Allocate the percentage of each feedstock to the total and add prefix to node id
    batteries_used_melted = batteries_used.melt(
        id_vars=["node_id", "country", "company", "product_type", year],
        value_vars=cathode_types[:-2],
        var_name="Feedstock",
        value_name="Percentage",
    )
    batteries_used_melted[year] = (
        batteries_used_melted["Percentage"] * batteries_used_melted[year]
    )
    batteries_used_melted = batteries_used_melted.drop(columns=["Percentage"])
    prefix_values = [str(i) for i in range(10, 81, 10)]  # Define prefix order
    prefix_map = dict(zip(cathode_types[:-2], prefix_values))  # Create dynamic mapping
    batteries_used_melted["node_id"] = (
        batteries_used_melted["Feedstock"].map(prefix_map)
        + batteries_used_melted["node_id"].astype(str)
    ).astype(int)
    batteries_used = batteries_used_melted[batteries_used_melted[year] > 0]

    # Convert batteries from MWh to tonnes of cathode material
    cathode_MWh_t_conv = pull_from_gcs_excel(
        bucket,
        CATHODE_COMPOSITION_DIR + "Cathode_conversion_factors.xlsx",
        sheet_name="Sheet1",
    )
    cathode_MWh_t_conv = (
        cathode_MWh_t_conv[["Chemistry ", int(year)]]
        .dropna()
        .rename(columns={"Chemistry ": "Feedstock", int(year): "t/MWh"})
    )
    batteries_used_converted = batteries_used.merge(
        cathode_MWh_t_conv, on="Feedstock", how="left"
    )
    batteries_used_converted[year] = (
        batteries_used_converted[year] * batteries_used_converted["t/MWh"]
    )
    batteries_used = batteries_used_converted.drop(columns=["t/MWh"])

    ## Run allocation
    remaining_cathodes = cathodes_used.copy().set_index("node_id")
    remaining_batteries = batteries_used.copy().set_index("node_id")
    edges, remaining_cathodes, remaining_batteries = create_edge_combinations(
        cathodes_used,
        batteries_used,
        remaining_cathodes,
        remaining_batteries,
        input_cols,
        output_cols,
        edge_type,
        year,
        allocation_method=allocation_method,
        edge_properties={"edge_destination": "product_type_y"},
    )

    ## Save edges to file
    edges["source"] = [int(str(i)[2:]) for i in edges["source"]]
    edges["target"] = [int(str(i)[2:]) for i in edges["target"]]
    save_nonzero_edges(edges, year, bucket, edge_filename)

    ###

    batteries_used.groupby(["product_type", "Feedstock"]).sum()[year]
    remaining_batteries.groupby(["product_type", "Feedstock"]).sum()[year]
    remaining_cathodes.groupby(["product_type"]).sum()[year]

    ###

    # Find lithium content in batteries

    # Li: 6.941 g/mol, O: 15.999 g/mol, H: 1.008 g/mol, C: 12.011 g/mol (LiOH and Li2CO3)
    Li, O, H, C = 6.941, 15.999, 1.008, 12.011
    lithium_content_conversion = {
        "Lithium Hydroxide": Li / (Li + O + H),
        "Lithium Carbonate": 2 * Li / (2 * Li + C + 3 * O),
        "Lithium Hydroxide Monohydrate": Li / (Li + 2 * O + 3 * H),
    }

    # cathode_composition_path = "/Users/lukecullen/Library/CloudStorage/OneDrive-UniversityofCambridge/Post-doc/P3c/data/Cathode_composition/used_cathode_composition.xlsx"
    # cathode_composition = pd.read_excel(cathode_composition_path).dropna(subset=["Type"])

    cathode_composition = pull_from_gcs_excel(
        bucket,
        CATHODE_COMPOSITION_DIR + "used_cathode_composition.xlsx",
        sheet_name="Sheet1",
    )
    cathode_composition = cathode_composition.dropna(subset=["Type"])

    cathode_composition = cathode_composition.melt(
        id_vars=["Type"],
        value_vars=["Lithium Hydroxide", "Lithium Carbonate"],
        var_name="Feedstock",
        value_name="Percentage",
    )
    cathode_composition = cathode_composition[cathode_composition["Percentage"] > 0]

    # # Convert LiOH(H20) requirement to LiOH
    # cathode_composition['Feedstock'] = ['Lithium Hydroxide' if i == 'Lithium Hydroxide Monohydrate' else i for i in cathode_composition['Feedstock']]
    # cathode_composition['conversion_factor'] = cathode_composition['Feedstock'].map({'Lithium Carbonate': 1, 'Lithium Hydroxide': lithium_content_conversion['Lithium Hydroxide Monohydrate']/lithium_content_conversion['Lithium Hydroxide']})
    # cathode_composition['Percentage'] = cathode_composition['Percentage'] * cathode_composition['conversion_factor']

    cathode_composition["Li_content"] = (
        cathode_composition["Feedstock"].map(lithium_content_conversion)
        * cathode_composition["Percentage"]
    )

    # In remaining batteries
    remaining_batteries_lithium = remaining_batteries.copy()
    remaining_batteries_lithium = remaining_batteries_lithium.merge(
        cathode_composition, left_on="Feedstock", right_on="Type", how="left"
    )
    remaining_batteries_lithium["Li_mass"] = (
        remaining_batteries_lithium[year] * remaining_batteries_lithium["Li_content"]
    )
    print(
        "Total lithium content in remaining batteries: "
        + str(remaining_batteries_lithium["Li_mass"].sum())
    )

    # In all batteries
    batteries_used_lithium = batteries_used.copy()
    batteries_used_lithium = batteries_used_lithium.merge(
        cathode_composition, left_on="Feedstock", right_on="Type", how="left"
    )
    batteries_used_lithium["Li_mass"] = (
        batteries_used_lithium[year] * batteries_used_lithium["Li_content"]
    )
    print(
        "Total lithium content in all batteries: "
        + str(batteries_used_lithium["Li_mass"].sum())
    )

    # Percentage matched
    print(
        f"Unmatched batteries: {(remaining_batteries_lithium['Li_mass'].sum()/batteries_used_lithium['Li_mass'].sum())*100:.1f}%"
    )

    print(
        f"Unallocated source cathode material: {(remaining_cathodes[year].sum()/cathodes_used[year].sum())*100:.1f}%"
    )

    # Lithium content in each battery based on inputs -> Used for end_use

    ####
    remaining_batteries_lithium

    ####
    batteries_power = batteries_used_lithium.merge(
        cathode_MWh_t_conv, left_on="Feedstock_x", right_on="Feedstock", how="left"
    )
    batteries_power["MWh"] = batteries_power[year] / batteries_power["t/MWh"]
    battery_lithium_power_vals = (
        batteries_power.groupby(["product_type"]).sum()["Li_mass"]
        / batteries_power.groupby(["product_type"]).sum()["MWh"]
    )

    # battery_lithium_power_vals.to_csv(
    #     "/Users/lukecullen/Library/CloudStorage/OneDrive-UniversityofCambridge/Post-doc/P3c/data/Cathode_composition/battery_Li_type_composition.csv"
    # )
    output_path = "/numerical_data_article/"
    battery_lithium_power_vals.to_csv(
        project_root + output_path + "battery_Li_type_composition.csv"
    )

    ###
    batteries_used.groupby(["product_type", "Feedstock"]).sum()[year]
    cathode_MWh_t_conv
    allocated_batteries = (
        batteries_used.groupby(["product_type", "Feedstock"]).sum()[year]
        - remaining_batteries.groupby(["product_type", "Feedstock"]).sum()[year]
    )
    total_allocation_per_product = (
        allocated_batteries.reset_index()
        .drop(columns="Feedstock")
        .groupby("product_type")
        .sum()
    )
    remaining_batteries.groupby(["product_type", "Feedstock"]).sum()[year].reset_index()
    remaining_batteries
    batteries_used.groupby(["product_type"]).sum()[year].reset_index()


def raw_material_edges():
    """
    Create edges between raw material and processing nodes based on the percentage of each feedstock
    """

    ## Variables
    year = "2023"

    edge_filename = "raw_material_edges.csv"

    feedstock_name_adjustments = {
        "Brine/DLE": "Brine",
        "Spodumene, Petalite": "Spodumene",
    }

    input_cols = ["mine_type", "country", "company"]
    output_cols = ["Feedstock", "country", "company"]
    edge_type = "mine_type"

    input_cols_cth = ["process_type", "country", "company"]
    edge_type_cth = "process_type_x"

    allocation_method = "even"  # 'even' or 'first come first serve'

    ## Import nodes
    bucket, nodes = import_operating_nodes(year)

    ## Split out mines and carbonate/hydroxide steps
    mines = nodes.dropna(subset=["mine_type"])
    processing = nodes.dropna(subset=["process_type"])

    mines_used = mines[["node_id", "country", "company", "mine_type", year]]

    processing_used = processing[
        ["node_id", "country", "company", "Feedstock", "process_type", year]
    ]
    processing_used = processing_used.replace({"Feedstock": feedstock_name_adjustments})

    carbonate_used = processing_used[
        processing_used["process_type"] == "Lithium Carbonate"
    ]
    carbonate_used = carbonate_used[carbonate_used["Feedstock"] != "Lithium Carbonate"]

    hydroxide_used = processing_used[
        processing_used["process_type"] == "Lithium Hydroxide"
    ]
    hydroxide_used = hydroxide_used[hydroxide_used["Feedstock"] != "Lithium Hydroxide"]

    ## Run allocation
    # Mines to carbonates
    remaining_mines = mines_used.copy().set_index("node_id")
    remaining_carbonates = carbonate_used.copy().set_index("node_id")

    edges, remaining_mines, remaining_carbonates = create_edge_combinations(
        mines_used,
        carbonate_used,
        remaining_mines,
        remaining_carbonates,
        input_cols,
        output_cols,
        edge_type,
        year,
        allocation_method=allocation_method,
    )

    # Mines to hydroxides
    remaining_hydroxides = hydroxide_used.copy().set_index("node_id")

    edges, remaining_mines, remaining_hydroxides = create_edge_combinations(
        mines_used,
        hydroxide_used,
        remaining_mines,
        remaining_hydroxides,
        input_cols,
        output_cols,
        edge_type,
        year,
        edges,
        allocation_method=allocation_method,
    )

    # Carbonates to hydroxides
    remaining_carbonates_source = carbonate_used.copy().set_index("node_id")

    edges, remaining_carbonates_source, remaining_hydroxides = create_edge_combinations(
        carbonate_used,
        hydroxide_used,
        remaining_carbonates_source,
        remaining_hydroxides,
        input_cols_cth,
        output_cols,
        edge_type_cth,
        year,
        edges,
        allocation_method=allocation_method,
    )

    ## Save edges to file
    save_nonzero_edges(edges, year, bucket, edge_filename)

    ## Save carbonate usage for next phase
    push_to_gcs_csv(
        remaining_carbonates_source.reset_index(),
        bucket,
        GCLOUD_PREPROCESSED_DIR
        + BENCHMARK_PREPROCESSED_DIR
        + BENCHMARK_EDGES_DIR
        + "remaining_carbonates_after_hydroxide.csv",
    )

    print(
        f"Unallocated source mine material: {(remaining_mines[year].sum()/mines_used[year].sum())*100:.2f}%"
    )
    print(
        f"Unmatched target carbonate material: {(remaining_carbonates[year].sum()/carbonate_used[year].sum())*100:.2f}%"
    )
    print(
        f"Unmatched source carbonate material: {(remaining_carbonates_source[year].sum()/carbonate_used[year].sum())*100:.2f}%"
    )
    print(
        f"Unmatched target hydroxide material: {(remaining_hydroxides[year].sum()/hydroxide_used[year].sum())*100:.2f}%"
    )


def endUse_material_edges():

    # Variables

    year = "2023"

    edge_filename = "endUse_material_edges.csv"

    battery_types = [
        "Cylindrical",
        "Pouch",
        "Cylindrical, Pouch",
        "Pouch, Prismatic",
        "Prismatic",
        "Cylindrical, Prismatic",
        "Cylindrical, Pouch, Prismatic",
    ]

    applications_path = "/Users/lukecullen/Library/CloudStorage/OneDrive-UniversityofCambridge/Post-doc/P3c/data/Lithium-ion_batteries/Lithium-ion-Batteries-Database-Q3-2024-4.xlsm"

    region_mapping_path = "/Users/lukecullen/Library/CloudStorage/OneDrive-UniversityofCambridge/Post-doc/P3c/data/Mappings/iso_region_mapping.csv"

    sheets = ["EV - Demand", "ESS - Demand", "Portables - Demand"]

    input_cols = ["Application(s)", "region"]
    output_cols = ["product_type", "region"]
    edge_type = "product_type_x"

    mixed_output_1 = {"EV": "EV, Portable", "Portable": "EV, Portable"}
    mixed_output_2 = {"EV": "EV, ESS", "ESS": "EV, ESS"}
    mixed_output_3 = {"Portable": "ESS, Portable", "ESS": "ESS, Portable"}

    allocation_method = "even"  # 'even' or 'first come first serve'

    ## Load data
    bucket, nodes = import_operating_nodes(year)

    ## Split out batteries and applications
    # Applications
    applications = pd.DataFrame()

    for sheet in sheets:
        df = pd.read_excel(applications_path, sheet_name=sheet, header=7)
        geo_index = df[df[df.columns[1]] == "Geography:"].index[0]
        df_filtered = df.iloc[geo_index : geo_index + 6].copy()
        df_filtered["Application"] = sheet.split(" - ")[0]
        applications = pd.concat((applications, df_filtered), ignore_index=True)

    applications = applications[["Segment", "Application", int(year)]].rename(
        columns={"Segment": "region", "Application": "product_type", int(year): year}
    )

    # Map countries to regions for end uses
    region_mapping = pd.read_csv(region_mapping_path)
    applications = applications.reset_index().rename(columns={"index": "node_id"})
    applications["product_type"] = applications["product_type"].replace(
        "Portables", "Portable"
    )
    applications["node_id"] = applications["node_id"] + 1000000
    applications["product_type_orig"] = applications["product_type"]

    # Batteries
    batteries = nodes[[i in battery_types for i in nodes["product_type"]]]
    batteries_used = pd.merge(batteries, region_mapping, on="country", how="left")
    batteries_used = batteries_used[
        [
            "node_id",
            "region",
            "country",
            "company",
            "product_type",
            "Application(s)",
            year,
        ]
    ]

    ## Run allocation
    edges = pd.DataFrame()

    remaining_source = batteries_used.copy().set_index("node_id")
    remaining_target = applications.copy().set_index("node_id")

    # For region specific types
    print(
        "Total source volume: "
        + str(remaining_source[year].sum())
        + ", Total target volume: "
        + str(remaining_target[year].sum())
    )

    edges, remaining_source, remaining_target = run_sequence_of_end_use_mixes(
        edges,
        batteries_used,
        applications,
        remaining_source,
        remaining_target,
        input_cols,
        output_cols,
        year,
        edge_type,
    )

    # For non-region specific types

    input_cols = ["Application(s)"]
    output_cols = ["product_type"]

    print(
        "Remaining source volume: "
        + str(remaining_source[year].sum())
        + ", Remaining target volume: "
        + str(remaining_target[year].sum())
    )

    edges, remaining_source, remaining_target = run_sequence_of_end_use_mixes(
        edges,
        batteries_used,
        applications,
        remaining_source,
        remaining_target,
        input_cols,
        output_cols,
        year,
        edge_type,
    )

    ## Save edges to file
    save_nonzero_edges(edges, year, bucket, edge_filename)

    ## Save nodes to file
    push_to_gcs_csv(
        applications.drop(columns=["product_type"]).rename(
            columns={"product_type_orig": "product_type"}
        ),
        bucket,
        GCLOUD_PREPROCESSED_DIR + BENCHMARK_PREPROCESSED_DIR + "endUse_nodes.csv",
    )

    nodes
    ## Concatenate edge files
    edge_files = [
        "raw_material_edges.csv",
        "cathode_material_edges.csv",
        "battery_material_edges.csv",
        "endUse_material_edges.csv",
    ]
    concatenate_edge_files(edge_files)


def cathode_material_edges():

    # Variables
    year = "2023"

    edge_filename = "cathode_material_edges.csv"

    # cathode_composition_path = '/Users/lukecullen/Library/CloudStorage/OneDrive-UniversityofCambridge/Post-doc/P3c/data/Cathode_composition/Cathode_composition.xlsx'

    feedstock_name_adjustments = {
        "Brine/DLE": "Brine",
        "Spodumene, Petalite": "Spodumene",
    }

    input_cols = ["process_type", "country", "company"]
    output_cols = ["Feedstock", "country", "company"]
    edge_type = "process_type"

    cathode_types = [
        "NCM mid nickel",
        "LFP",
        "4V Ni or Mn based",
        "NCA",
        "NCM high nickel",
        "LCO",
        "Under Review",
        "NCM low nickel",
        "Other",
        "5V Mn based",
    ]

    # Li: 6.941 g/mol, O: 15.999 g/mol, H: 1.008 g/mol, C: 12.011 g/mol (LiOH and Li2CO3)

    Li, O, H, C = 6.941, 15.999, 1.008, 12.011

    lithium_content_conversion = {
        "Lithium Hydroxide": Li / (Li + O + H),
        "Lithium Carbonate": 2 * Li / (2 * Li + C + 3 * O),
        "Lithium Hydroxide Monohydrate": Li / (Li + 2 * O + 3 * H),
    }

    allocation_method = "even"  # 'even' or 'first come first served'

    ## Load data

    bucket, nodes = import_operating_nodes(year)

    # Load remaining cathode data

    remaining_carbonates_after_hydroxide = pull_from_gcs_csv(
        bucket,
        GCLOUD_PREPROCESSED_DIR
        + BENCHMARK_PREPROCESSED_DIR
        + BENCHMARK_EDGES_DIR
        + "remaining_carbonates_after_hydroxide.csv",
    )

    ## Split out processing and cathodes

    processing = nodes.dropna(subset=["process_type"])

    cathodes = nodes[[i in cathode_types for i in nodes["product_type"]]]

    # Filter processing nodes
    processing_used = processing[
        ["node_id", "country", "company", "Feedstock", "process_type", year]
    ]

    processing_used = processing_used.replace({"Feedstock": feedstock_name_adjustments})

    carbonate_used = processing_used[
        processing_used["process_type"] == "Lithium Carbonate"
    ]

    carbonate_used = carbonate_used[carbonate_used["Feedstock"] != "Lithium Carbonate"]

    hydroxide_used = processing_used[
        processing_used["process_type"] == "Lithium Hydroxide"
    ]

    hydroxide_used = hydroxide_used[hydroxide_used["Feedstock"] != "Lithium Hydroxide"]

    ## Filter cathode nodes
    cathodes_used = cathodes[
        ["node_id", "country", "operator_short_clean", "product_type", year]
    ].rename(columns={"operator_short_clean": "company"})

    # Add node id prefix based on cathode type due to multiple cathode types from some facilities

    prefix_values = [str(i) for i in range(10, 56, 5)]  # Define prefix order

    prefix_map = dict(zip(cathode_types, prefix_values))  # Create mapping
    cathodes_used["node_id"] = (
        cathodes_used["product_type"].map(prefix_map)
        + cathodes_used["node_id"].astype(str)
    ).astype(int)

    # Convert unit to tonnes

    lce_to_lhx = (
        lithium_content_conversion["Lithium Carbonate"]
        / lithium_content_conversion["Lithium Hydroxide"]
    )

    hydroxide_used[year] = hydroxide_used[year] * lce_to_lhx

    # cathode_composition = pd.read_excel(cathode_composition_path).dropna(subset=['Cathode_type'])

    cathode_composition = pull_from_gcs_excel(
        bucket,
        CATHODE_COMPOSITION_DIR + "Cathode_composition.xlsx",
        sheet_name="Sheet1",
    )
    cathode_composition = cathode_composition.dropna(subset=["Cathode_type"])

    cathode_composition = cathode_composition.melt(
        id_vars=["Cathode_type"],
        value_vars=["Lithium Hydroxide Monohydrate", "Lithium Carbonate"],
        var_name="Feedstock",
        value_name="Percentage",
    )
    cathode_composition = cathode_composition[cathode_composition["Percentage"] > 0]

    # Convert LiOH(H20) requirement to LiOH
    cathode_composition["Feedstock"] = [
        "Lithium Hydroxide" if i == "Lithium Hydroxide Monohydrate" else i
        for i in cathode_composition["Feedstock"]
    ]
    cathode_composition["conversion_factor"] = cathode_composition["Feedstock"].map(
        {
            "Lithium Carbonate": 1,
            "Lithium Hydroxide": lithium_content_conversion[
                "Lithium Hydroxide Monohydrate"
            ]
            / lithium_content_conversion["Lithium Hydroxide"],
        }
    )
    cathode_composition["Percentage"] = (
        cathode_composition["Percentage"] * cathode_composition["conversion_factor"]
    )

    cathodes_merged = cathodes_used.merge(
        cathode_composition, left_on="product_type", right_on="Cathode_type"
    )

    cathodes_merged[year] = cathodes_merged[year] * cathodes_merged["Percentage"]

    cathodes_used = cathodes_merged.drop(columns=["Cathode_type", "Percentage"])

    # Print total lithium mass in cathodes

    tot_weight = cathodes_merged.groupby("Feedstock").sum()[[year]]

    tot_weight["Lithium_content"] = tot_weight.index.map(lithium_content_conversion)

    tot_weight["Lithium_mass"] = tot_weight[year] * tot_weight["Lithium_content"]

    print(f"Lithium mass of cathodes: {tot_weight.sum()['Lithium_mass']:.0f} tonnes")

    ## Run allocation

    # Run for lithium carbonate -> Cathodes

    remaining_carbonate = remaining_carbonates_after_hydroxide.copy().set_index(
        "node_id"
    )

    remaining_cathode = cathodes_used.copy().set_index("node_id")

    remaining_cathode_init = remaining_cathode.copy()

    edges, remaining_carbonate, remaining_cathode = create_edge_combinations(
        carbonate_used,
        cathodes_used,
        remaining_carbonate,
        remaining_cathode,
        input_cols,
        output_cols,
        edge_type,
        year,
        allocation_method=allocation_method,
        edge_properties={"edge_destination": "product_type"},
    )

    remaining_cathode_mid = remaining_cathode.copy()

    # Convert remaining cathodes to lithium hydroxide from lithium carbonate for those that are interchangeable (mid nickel and umbrella categories)

    carbonate_to_hydroxide_conversion = pd.read_excel(cathode_composition_path)
    carbonate_to_hydroxide_conversion = carbonate_to_hydroxide_conversion.dropna(
        subset=["Benchmark_name"]
    ).set_index("Benchmark_name")

    cth_conv_factor = (
        carbonate_to_hydroxide_conversion.loc[
            "NCM 622", "Lithium Hydroxide Monohydrate"
        ]
        / carbonate_to_hydroxide_conversion.loc["NCM 523", "Lithium Carbonate"]
    ) * (
        lithium_content_conversion["Lithium Hydroxide Monohydrate"]
        / lithium_content_conversion["Lithium Hydroxide"]
    )

    remaining_cathode_for_hydroxide = remaining_cathode.copy()

    remaining_cathode_for_hydroxide[
        "conversion_factor"
    ] = remaining_cathode_for_hydroxide["product_type"].apply(
        lambda x: cth_conv_factor if x in ["4V Ni or Mn based", "NCM mid nickel"] else 1
    )

    remaining_cathode_for_hydroxide[year] = (
        remaining_cathode_for_hydroxide[year]
        * remaining_cathode_for_hydroxide["conversion_factor"]
    )

    remaining_cathode_for_hydroxide["Feedstock"] = [
        (
            "Lithium Hydroxide"
            if i in ["4V Ni or Mn based", "NCM mid nickel"]
            else remaining_cathode_for_hydroxide["Feedstock"].iloc[num]
        )
        for num, i in enumerate(remaining_cathode_for_hydroxide["product_type"])
    ]

    cathodes_used_for_hydroxide = cathodes_used.copy()

    cathodes_used_for_hydroxide["Feedstock"] = [
        (
            "Lithium Hydroxide"
            if i in ["4V Ni or Mn based", "NCM mid nickel"]
            else cathodes_used_for_hydroxide["Feedstock"].iloc[num]
        )
        for num, i in enumerate(cathodes_used_for_hydroxide["product_type"])
    ]

    remaining_cathode_for_hydroxide = remaining_cathode_for_hydroxide.drop(
        columns=["conversion_factor"]
    )

    # Run for lithium hydroxide -> Cathodes

    remaining_hydroxide = hydroxide_used.copy().set_index("node_id")

    remaining_cathode = remaining_cathode_for_hydroxide.copy()

    # input_cols = ['process_type', 'country', 'company']

    # output_cols = ['Feedstock', 'country', 'company']

    # edge_type = 'process_type'

    edges, remaining_hydroxide, remaining_cathode = create_edge_combinations(
        hydroxide_used,
        cathodes_used_for_hydroxide,
        remaining_hydroxide,
        remaining_cathode,
        input_cols,
        output_cols,
        edge_type,
        year,
        edges,
        allocation_method=allocation_method,
        edge_properties={"edge_destination": "product_type"},
    )

    ## Save edges to file
    edges["target"] = [int(str(i)[2:]) for i in edges["target"]]

    # # Combine edges to same cathode facility - multiple edges can occur due to multiple cathode type produced at same facility

    # combined_cathode_type_edges = edges.drop(columns='properties').groupby(['source', 'target', 'edge_type']).sum().reset_index()

    # combined_cathode_type_edges['properties'] = len(combined_cathode_type_edges) * [{}]

    save_nonzero_edges(edges, year, bucket, edge_filename)

    ## Print lithium contents

    remaining_cathode_lithium = remaining_cathode.copy()
    remaining_cathode_lithium["Li_conv"] = remaining_cathode_lithium["Feedstock"].map(
        lithium_content_conversion
    )
    remaining_cathode_lithium["Li_content"] = (
        remaining_cathode_lithium[year] * remaining_cathode_lithium["Li_conv"]
    )

    print(
        "Total lithium content in cathodes: "
        + str(remaining_cathode_lithium["Li_content"].sum())
    )

    print(
        f"Unmatched lithium content in cathodes: {(remaining_cathode_lithium['Li_content'].sum()/tot_weight.sum()['Lithium_mass'])*100:.1f}%"
    )

    print("Unallocated material tonnage:")

    remaining_cathode_lithium.groupby("Feedstock").sum()[year]


def Li_edge_calculation():

    year = "2023"

    bucket, nodes = import_operating_nodes(year)

    edges = pull_from_gcs_csv(
        bucket,
        GCLOUD_PREPROCESSED_DIR
        + BENCHMARK_PREPROCESSED_DIR
        + BENCHMARK_EDGES_DIR
        + BENCHMARK_EDGES_FILE,
    )

    stages_dict = {
        "mining": ["Brine", "Spodumene", "Mica", "Pegmatite"],
        "carbonate": ["Lithium Carbonate"],
        "hydroxide": ["Lithium Hydroxide"],
        "cathode": [
            "NCM mid nickel",
            "LFP",
            "4V Ni or Mn based",
            "NCA",
            "NCM high nickel",
            "LCO",
            "NCM low nickel",
            "5V Mn based",
        ],
        "battery": [
            "Cylindrical",
            "Pouch",
            "Cylindrical, Pouch",
            "Pouch, Prismatic",
            "Prismatic",
            "Cylindrical, Prismatic",
            "Cylindrical, Pouch, Prismatic",
        ],
        "end_use": ["EV", "ESS", "Portable"],
    }

    edges

    inputs = edges.merge(
        nodes_df[["node_id", "type", "stage"]],
        left_on=["source", "edge_type"],
        right_on=["node_id", "type"],
        how="left",
    )
    outputs = inputs[["stage", "type", "target", "2023_volume", "edge_type"]].merge(
        nodes_df[["node_id", "stage", "type"]],
        left_on="target",
        right_on="node_id",
        how="left",
        suffixes=("_source", "_target"),
    )
    all_flows = (
        outputs[
            [
                "stage_source",
                "type_source",
                "stage_target",
                "type_target",
                "2023_volume",
                "edge_type",
            ]
        ]
        .groupby(
            ["stage_source", "type_source", "stage_target", "type_target", "edge_type"]
        )
        .sum()
        .reset_index()
    )

    stages = list(stages_dict.keys())
    next_stage_map = {stages[i]: stages[i + 1] for i in range(len(stages) - 1)}

    # 2. Keep only rows where stage_target matches next_stage_map[stage_source]
    real_flows = pd.concat(
        (
            all_flows[
                all_flows["stage_source"].map(next_stage_map)
                == all_flows["stage_target"]
            ],
            all_flows[
                (all_flows["stage_source"] == "mining")
                & (all_flows["stage_target"] == "hydroxide")
            ],
            all_flows[
                (all_flows["stage_source"] == "carbonate")
                & (all_flows["stage_target"] == "cathode")
            ],
        )
    )

    unit_conversion = pd.read_excel(
        "/Users/lukecullen/Library/CloudStorage/OneDrive-UniversityofCambridge/Post-doc/P3c/data/Mappings/Li_unit_conversion.xlsx"
    )

    up_to_cathodes = real_flows[
        [
            i in ["mining", "carbonate", "hydroxide", "cathode"]
            for i in real_flows["stage_target"]
        ]
    ]
    up_to_cathodes = up_to_cathodes.merge(
        unit_conversion[["type", "edge_conversion"]],
        left_on="type_target",
        right_on="type",
        how="left",
    )
    up_to_cathodes["2023_volume"] = (
        up_to_cathodes["2023_volume"] * up_to_cathodes["edge_conversion"]
    )

    beyond_cathodes = real_flows[
        [i in ["battery", "end_use"] for i in real_flows["stage_target"]]
    ]
    beyond_cathodes = beyond_cathodes.merge(
        unit_conversion[["type", "edge_conversion"]],
        left_on="edge_type",
        right_on="type",
        how="left",
    )
    beyond_cathodes["2023_volume"] = (
        beyond_cathodes["2023_volume"] * beyond_cathodes["edge_conversion"]
    )

    real_flows = pd.concat((up_to_cathodes, beyond_cathodes))


if __name__ == "__main__":
    battery_material_edges()
    raw_material_edges()
    endUse_material_edges()
    cathode_material_edges()
