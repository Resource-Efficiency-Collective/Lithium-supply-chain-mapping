import unicodedata
import re


KNOWN_SUFFIXES = {
    "co ltd": "Co., Ltd.",
    "co., ltd": "Co., Ltd.",
    "corporation": "Corp.",
    "corp": "Corp.",
    "inc": "Inc.",
    "inc.": "Inc.",
    "limited": "Ltd.",
    "ltd": "Ltd.",
    "ltd.": "Ltd.",
    "plc": "PLC",
    "ag": "AG",
    "llc": "LLC",
    "llc.": "LLC",
    "group": "Group",
    "holding": "Holding",
    "bv": "BV",
    "sa": "SA",
    "pty ltd": "Pty Ltd.",
    "pte ltd": "Pte Ltd.",
    "llp": "LLP",
    "sarl": "SARL",
    "spa": "SPA",
    "kg": "KG",
    "s.a.": "S.A.",
    "s.p.a.": "S.p.A.",
    "gmbh": "GmbH",
}

UPPERCASE_WHITELIST = {
    "USA",
    "AG",
    "PLC",
    "LLC",
    "BV",
    "SA",
    "SPA",
    "KG",
    "S.A.",
    "ABEE",
    "IFC",
    "GM",
    "NPUC",
    "ESM",
    "PME",
    "NKS",
    "CATL",
    "BRUNP",
    "CMOC",
    "CBL",
    "HXR",
    "TQL",
    "CBMM",
    "REPT",
    "SVOLT",
    "LGES",
    "NIO",
    "ALB-MIN",
    "AM&T",
    "E&D",
    "GMBH",
    "XPENG",
    "FREYR",
    "GALP",
    "LICO",
    "SK",
    "PACCAR",
    "ERLOS",
    "ACE",
    "LOHUM",
    "POSCO",
    "HELM",
}

CASE_SENSITIVE_EXCEPTIONS = {
    "Maxmind": "MaxMind",
    "Ecopro": "EcoPro",
    "Cosmx": "CosMX",
    "Inobat": "InoBat",
    "Ecopro Bm": "EcoPro BM",
}

LOWERCASE_WORDS = {"de", "del", "and", "of"}


def normalize_name(name: str) -> str:
    # Unicode normalization (NFKC)
    name = unicodedata.normalize("NFKC", name)
    # Replace non-breaking spaces
    name = name.replace("\xa0", " ")
    # Remove newlines
    name = name.replace("\n", " ")
    # Collapse multiple spaces
    name = " ".join(name.strip().split())
    return name


def clean_parentheses(name: str) -> str:
    # Ensure space before '(' and after ')' when needed
    name = re.sub(r"\s*\(\s*", " (", name)
    name = re.sub(r"\s*\)\s*", ") ", name)
    name = " ".join(name.split())
    return name


def standardize_suffix(name: str) -> str:
    """
    Standardize suffixes by checking the last word(s) and ensuring proper punctuation.
    """
    parts = name.split()
    if not parts:
        return name

    # Check last two words first
    if len(parts) >= 2:
        last_two = " ".join(parts[-2:]).lower().rstrip(".")
        if last_two in KNOWN_SUFFIXES:
            parts = parts[:-2] + [KNOWN_SUFFIXES[last_two]]
            return " ".join(parts)

    # Check last word
    last_word = parts[-1].lower().rstrip(".")
    if last_word in KNOWN_SUFFIXES:
        parts[-1] = KNOWN_SUFFIXES[last_word]
        return " ".join(parts)

    # Attempt simple normalization for common endings
    common_endings = {"co": "Co.", "inc": "Inc.", "corp": "Corp.", "ltd": "Ltd."}
    if last_word in common_endings:
        parts[-1] = common_endings[last_word]

    # Join with no extra space before commas
    return ", ".join(" ".join(parts).split(","))


def normalize_separators(name: str) -> str:
    """
    Normalize separators: ensure single spaces around '/', '&', and '-'.
    Remove any spaces before commas.
    """
    # Replace multiple hyphens with single hyphen
    name = re.sub(r"-{2,}", "-", name)

    # Ensure spaces around '/', '&'
    name = re.sub(r"\s*/\s*", " / ", name)
    name = re.sub(r"\s*&\s*", " & ", name)

    # Remove spaces before commas and ensure single space after commas
    name = re.sub(r"\s*,\s*", ", ", name)

    # Collapse multiple '+' signs into a single '+'
    name = re.sub(r"\++", "+", name)

    # Remove extra spaces
    name = " ".join(name.split())

    return name


def reconstruct_word(original: str, transformed: str) -> str:
    """
    Reconstruct punctuation around a transformed word.
    """
    prefix = re.match(r"^\W+", original)
    prefix_str = prefix.group(0) if prefix else ""
    suffix = re.search(r"\W+$", original)
    suffix_str = suffix.group(0) if suffix else ""
    return prefix_str + transformed + suffix_str


def handle_acronyms_and_case(word: str) -> str:
    """
    Preserve acronyms, uppercase whitelist, and properly title-case.
    Steps:
    - If word (stripped of punctuation) is in UPPERCASE_WHITELIST, keep uppercase.
    - If word is all uppercase or short (<=4 chars) uppercase, keep uppercase.
    - If word in LOWERCASE_WORDS, keep lowercase.
    - Else:
      - If original was all lowercase, Title case it.
      - If original had mixed case, preserve original form.
    """
    # Handle parentheses separately
    if word.startswith("(") and word.endswith(")") and len(word) > 2:
        inner = word[1:-1]
        processed_inner = handle_acronyms_and_case(inner)
        return f"({processed_inner})"

    # Save original form for possible mixed-case preservation
    original = word
    # Strip punctuation for checks
    stripped = re.sub(r"^\W+|\W+$", "", word)

    if not stripped:  # If empty after stripping punctuation
        return word

    if stripped in CASE_SENSITIVE_EXCEPTIONS.values():
        return original

        # Check if the word is in the "CASE_SENSITIVE_EXCEPTIONS"
    if stripped in CASE_SENSITIVE_EXCEPTIONS:
        return reconstruct_word(original, CASE_SENSITIVE_EXCEPTIONS[stripped])

    # Check uppercase whitelist
    if stripped.upper() in UPPERCASE_WHITELIST:
        return reconstruct_word(original, stripped.upper())

    # Check lowercase list
    if stripped.lower() in LOWERCASE_WORDS:
        return reconstruct_word(original, stripped.lower())

    # Determine the casing pattern of the stripped word
    if stripped.isupper():
        # Keep as uppercase (for acronyms or known uppercase words)
        return reconstruct_word(original, stripped.upper())
    elif stripped.islower():
        # Title case (first letter uppercase, rest lowercase)
        return reconstruct_word(original, stripped.capitalize())
    else:
        # Mixed case: preserve original form
        return original


def title_case_preserving_acronyms(name: str) -> str:
    words = name.split()
    new_words = [handle_acronyms_and_case(w) for w in words]
    return " ".join(new_words)


def fix_company_name(name: str) -> str:
    # 1. Normalize and clean parentheses
    name = normalize_name(name)
    name = clean_parentheses(name)

    # 2. Standardize suffix
    name = standardize_suffix(name)

    # 3. Normalize separators to handle spaces around punctuation
    name = normalize_separators(name)

    # 4. Title case preserving acronyms and original mixed-case words
    name = title_case_preserving_acronyms(name)

    # 5. Final suffix check
    name = standardize_suffix(name)

    # Final normalization of spaces
    name = " ".join(name.split())

    return name


###


short_names = {
    "Gotion High-tech": "Gotion",
    "Gotion High Tech": "Gotion",
    "Gotion High-Tech": "Gotion",
    "Li-Cycle Holdings": "Li-Cycle",
    "Li-Cycle Ontario Spoke": "Li-Cycle",
    "Li-Cycle Germany Spoke": "Li-Cycle",
    "Li-Cycle New York Hub": "Li-Cycle",
    "Li-Cycle Alabama Spoke": "Li-Cycle",
    "LG Chem,": "LG Chem",
    "LG Energy Solution": "LG Chem",
    "LGES": "LG Chem",
    "LG Energy Solution,": "LG Chem",
    "Panasonic Energy": "Panasonic",
    "Panasonic Energy of North America": "Panasonic",
    "Panasonic Holdings": "Panasonic",
    "BYD Auto Industry Company": "BYD",
    "BYD Co., Ltd.": "BYD",
    "BYD HK": "BYD",
    "Tianqi (TQL)": "Tianqi Lithium",
    "Tianqi Holdings": "Tianqi Lithium",
    "Geely Auto": "Geely",
    "Geely Automobile Group": "Geely",
    "Geely Automobile Holdings": "Geely",
    "Geely Technology Group": "Geely",
    "Zhejiang Geely Automobile": "Geely",
    "Mitsui &": "Mitsui",
    "Mitsui & Co.": "Mitsui",
    "EVE Energy": "EVE",
    "BTR New Energy Materials Co., Ltd.": "BTR",
    "Microvast,": "Microvast",
    "Microvast Holdings,": "Microvast",
    "Redwood Materials": "Redwood",
    "Redwood Materials,": "Redwood",
    "Hunan Corun New Energy Co.": "Hunan Corun",
    "Hunan Corun New Energy Co (Corun)": "Hunan Corun",
    "LOHUM Cleantech Pvt.": "LOHUM",
    "LOHUM Cleantech Private": "LOHUM",
    "NIO Holdings": "NIO",
    "Ganfeng Lithium Group": "Ganfeng Lithium",
    "Jiangxi Ganfeng Lithium Industry Group": "Ganfeng Lithium",
    "Xinyu Ganfeng Mining": "Ganfeng Lithium",
    "Jiangxi Ganfeng Circulation Technology": "Ganfeng Lithium",
    "Avalon Advanced Materials": "Avalon",
    "Rio Tinto Exploration Canada": "Rio Tinto",
    "Lithium Americas (Argentina)": "Lithium Americas",
    "Lithium Americas Corp (LAC)": "Lithium Americas",
    "Aqua Metals,": "Aqua Metals",
    "Faam": "FAAM",
    "GEM (Jiangsu) Cobalt": "GEM",
    "GEM Co., Ltd.": "GEM",
    "Ningbo Ronbay New Energy Technology": "Ningbo Ronbay",
    "Ningbo Ronbay Lithium Battery Materials": "Ningbo Ronbay",
    "Ningbo Shanshan New Energy Technology Development": "Ningbo Shanshan",
    "Xiamen Tungsten New Energy": "Xiamen Tungsten",
    "Xiamen Tungsten Co., Ltd.": "Xiamen Tungsten",
    "Tianjin Lishen Battery Joint-Stock": "Tianjin Lishen Battery",
    "FREYR Battery": "FREYR",
    "FREYR Battery,": "FREYR",
    "Li-Cycle Arizona Spoke": "Li-Cycle",
    "Li-Cycle France": "Li-Cycle",
    "Li-Cycle Norway AS": "Li-Cycle",
    "Li-Cycle New York Spoke": "Li-Cycle",
    "Yahua Lithium": "Yahua",
    "Yahua Industrial": "Yahua",
    "Contemporary Amperex Technology": "CATL",
    "Northvolt AB": "Northvolt",
    "Northvolt Fem": "Northvolt",
    "POSCO Chemical": "POSCO",
    "POSCO Future M": "POSCO",
    "POSCO-HY Clean Metal": "POSCO",
    "Envision AESC": "Envision AESC",
    "Envision AESC Group": "Envision AESC",
    "Envision Energy": "Envision",
    "Samsung SDI": "Samsung",
    "Rio Tino": "Rio Tinto",
    "General Motors Co.": "General Motors",
    "Tianqi Lithium": "Tianqi",
    "Zhejiang Huayou Cobalt": "Huayou",
    "LG Energy Solutions": "LG Chem",
    "LG Energy": "LG Chem",
}


###


def clean_company_name(company_list):
    cleaned_list = []
    for company in company_list:
        cleaned_list.append(fix_company_name(company))
    return cleaned_list


def shorten_company_name(company_list):
    SUFFIXES = set(KNOWN_SUFFIXES.values())
    # Compile a regex pattern for the suffixes (anchored to end of string)
    pattern = re.compile(
        rf"\s?({'|'.join(re.escape(suffix) for suffix in SUFFIXES)})$", re.IGNORECASE
    )
    # Process the list of names
    list_without_suffixes = [pattern.sub("", name).strip() for name in company_list]

    short_names_list = [short_names.get(name, name) for name in list_without_suffixes]

    return short_names_list
