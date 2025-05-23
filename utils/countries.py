import country_converter as coco
import warnings
import pandas as pd

cc = coco.CountryConverter()

dict_to_iso3 = dict(
    zip(cc.name_shortas("ISO3")["name_short"], cc.name_shortas("ISO3")["ISO3"])
)
dict_to_iso3.update(
    dict(
        zip(
            cc.name_officialas("ISO3")["name_official"],
            cc.name_officialas("ISO3")["ISO3"],
        )
    )
)

dict_to_iso3.update(
    {
        "Bolivia (Plurinational State of)": "BOL",
        "China, Hong Kong SAR": "HKG",
        "Congo": "COG",
        "Dem. Rep. of the Congo": "COD",
        "Dominican Rep.": "DOM",
        "Kyrgyzstan": "KGZ",
        "Lao People's Dem. Rep.": "LAO",
        "Rep. of Korea": "KOR",
        "Russian Federation": "RUS",
        "State of Palestine": "PSE",
        "United Rep. of Tanzania": "TZA",
        "Viet Nam": "VNM",
        "Other Asia, nes": "OAS",
        "Bonaire": "BES",
        "Br. Virgin Isds": "VGB",
        "Côte d'Ivoire": "CIV",
        "Republic of Ireland": "IRL",
        "Northern Cape": "ZAF",
        "UK": "GBR",
        "DRC": "COD",
        "UAE": "ARE",
        "Taiwan, Province of China": "TWN",
        "Turkey": "TUR",
        "FInland": "FIN",
    }
)


def rename_countries(df, col):
    """
    Renames country names in the specified column of the DataFrame to ISO 3-digit codes
    if they are not already in that format. For unmatched values, the original value is retained,
    and a warning is issued to update the dictionary unless the value is NaN.

    Parameters:
    df (pd.DataFrame): The DataFrame containing country names.
    col (str): The column name in the DataFrame to process.

    Returns:
    pd.DataFrame: The updated DataFrame with country names converted to ISO 3-digit codes.
    """
    valid_iso3_codes = list(cc.name_officialas("ISO3")["ISO3"])

    def convert_to_iso3(country):
        # Ignore NaN values
        if pd.isna(country):
            return country
        # Check if the country is already in the valid ISO3 list
        if country in valid_iso3_codes:
            return country
        # Try to convert using the dictionary
        iso3 = dict_to_iso3.get(country)
        if iso3:
            return iso3
        else:
            # Warn if the country is not found and retain the original value
            warnings.warn(
                f"Country '{country}' not found in the dictionary. Please update dict_to_iso3."
            )
            return country

    # Apply the conversion function to the specified column
    df[col] = df[col].apply(convert_to_iso3)
    return df
