import json
import pandas as pd
from typing import Dict

from preprocessing.benchmark_formatting import read_benchmark
from utils.gpt_match_responses import *

# def combine_matches(match_dataframes: list) -> dict:
#
#     # combine matches
#     partnership_matches = {}
#     for i in range(len(match_dataframes)):
#         if i == 0:
#             continue
#         for key in match_dataframes[i - 1].keys():
#             # Combine the lists and remove duplicates by converting to a set, then back to a list
#             combined = list(
#                 set(match_dataframes[i - 1][key] + match_dataframes[i][key])
#             )
#             partnership_matches[key] = combined
#
#     return partnership_matches


def merge_dicts(*dicts):
    """
    Combines multiple dictionaries and removes duplicate keys while keeping all unique values
    under each key.

    :param dicts: The dictionaries to be merged.
    :return: A merged dictionary with unique keys and lists of unique values.
    """
    merged_dict = {}

    for d in dicts:
        for key, value in d.items():
            if key not in merged_dict:
                # If key is not already in merged_dict, add it
                merged_dict[key] = set()  # Use a set to keep values unique
            if isinstance(value, list):
                merged_dict[key].update(value)
            else:
                merged_dict[key].add(value)

    # Convert sets back to lists
    return {key: list(values) for key, values in merged_dict.items()}


def hallucination_test(
    step0: pd.DataFrame,
    step0_matches: Dict[str, list],
    step1: pd.DataFrame,
    step1_matches: Dict[str, list],
    partnership: pd.DataFrame,
) -> bool:
    """
    Validate that GPT-generated matches do not introduce hallucinated keys or values.

    Args:
        step0 (pd.DataFrame): Initial dataset with assets.
        step0_matches (dict): GPT-generated matches for step0.
        step1 (pd.DataFrame): Processed dataset with assets.
        step1_matches (dict): GPT-generated matches for step1.
        partnership (pd.DataFrame): Dataset containing valid partnership assets.

    Returns:
        bool: True if all checks pass, False otherwise.
    """

    def validate_keys(
        matches: Dict[str, list], valid_keys: set, step_name: str
    ) -> bool:
        """Check that all keys in matches exist in valid_keys."""
        invalid_keys = [key for key in matches.keys() if key not in valid_keys]
        if invalid_keys:
            print(f"[FAIL] Invalid keys in {step_name}_matches: {invalid_keys}")
            return False
        print(f"[PASS] All keys in {step_name}_matches are valid.")
        return True

    def validate_values(
        matches: Dict[str, list], valid_values: set, step_name: str
    ) -> bool:
        """Check that all values in matches exist in valid_values."""
        values = [val for vals in matches.values() for val in vals]
        invalid_values = [val for val in values if val not in valid_values]
        if invalid_values:
            print(f"[FAIL] Invalid values in {step_name}_matches: {invalid_values}")
            return False
        print(f"[PASS] All values in {step_name}_matches are valid.")
        return True

    def validate_initial_matches(
        step: pd.DataFrame,
        matches: Dict[str, list],
        partnership: pd.DataFrame,
        step_name: str,
    ) -> bool:
        """Check that GPT matches contain all initial matches."""
        initial_matches = set(step["Asset Name"].dropna().values) & set(
            partnership["Asset Name"].dropna().values
        )
        missing_matches = [
            item for item in initial_matches if item not in matches.get(item, [])
        ]
        if missing_matches:
            print(
                f"[FAIL] Missing initial matches in {step_name}_matches: {missing_matches}"
            )
            return False
        print(f"[PASS] All initial matches in {step_name}_matches are valid.")
        return True

    # Validate step0_matches
    step0_checks = {
        "Keys Check": validate_keys(
            step0_matches, set(partnership["Asset Name"].dropna().values), "step0"
        ),
        "Values Check": validate_values(
            step0_matches, set(step0["Asset Name"].dropna().values), "step0"
        ),
        "Initial Matches Check": validate_initial_matches(
            step0, step0_matches, partnership, "step0"
        ),
    }

    # Validate step1_matches
    step1_checks = {
        "Keys Check": validate_keys(
            step1_matches, set(partnership["Asset Name"].dropna().values), "step1"
        ),
        "Values Check": validate_values(
            step1_matches, set(step1["Asset Name"].dropna().values), "step1"
        ),
        "Initial Matches Check": validate_initial_matches(
            step1, step1_matches, partnership, "step1"
        ),
    }

    # Combine results
    all_checks = {
        "step0": step0_checks,
        "step1": step1_checks,
    }

    # Print summary of results
    passed = True
    for step, checks in all_checks.items():
        print(f"\nResults for {step}:")
        for check_name, result in checks.items():
            if not result:
                passed = False
                print(f"  [FAIL] {check_name}")
            else:
                print(f"  [PASS] {check_name}")

    if passed:
        print("\nAll hallucination checks passed.")
    else:
        print("\nSome hallucination checks failed.")

    return passed


def save_dict_to_json(data: dict, filename: str, indent: int = 4) -> None:
    """
    Save a dictionary to a JSON file with optional formatting.

    Args:
        data (dict): The dictionary to save.
        filename (str): The name of the JSON file (e.g., 'output.json').
        indent (int, optional): The number of spaces for indentation in the JSON file. Default is 4.

    Raises:
        ValueError: If the provided data is not a dictionary.
        IOError: If there is an error writing to the file.
    """
    if not isinstance(data, dict):
        raise ValueError("The provided data must be a dictionary.")

    try:
        with open(filename, "w") as file:
            json.dump(data, file, indent=indent)
        print(f"Dictionary successfully saved to {filename}")
    except IOError as e:
        print(f"Error saving dictionary to {filename}: {e}")


if __name__ == "__main__":
    # Combine matches
    partnership_matches = merge_dicts(
        lithium_mines_matches,
        lithium_processing_matches,
        cathode_manufacture_matches,
        battery_manufacture_matches,
        recycling_matches,
    )

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
    ) = read_benchmark()

    # Check
    hallucination_test(
        lithium_mines,
        lithium_mines_matches,
        lithium_processing,
        lithium_processing_matches,
        lithium_partnership,
    )

    save_dict_to_json(
        partnership_matches, "preprocessing/matching_jsons/partnership_matching.json"
    )
