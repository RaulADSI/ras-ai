import pandas as pd
from rapidfuzz import process, fuzz
from typing import List, Optional, Tuple


def normalize(text: str) -> str:
    """
    Converts text to a normalized form for matching (lowercase, stripped of whitespace).
    """
    if not isinstance(text, str):
        return ""
    return text.lower().strip()


def is_ambiguous(value: object) -> bool:
    """
    Checks if a value is null, empty, or contains 'unknown', indicating it's ambiguous.
    """
    return pd.isna(value) or str(value).strip() == "" or "unknown" in str(value).lower()


def get_best_match(query: str, choices: List[str], score_cutoff: int = 50) -> Tuple[Optional[str], float]:
    """
    Finds the best fuzzy match for a query string from a list of choices.
    Returns a tuple: (best_match, score).
    """
    if is_ambiguous(query):
        return None, 0.0

    normalized_query = normalize(query)

    # Normalize the list of choices and preserve the mapping to originals
    normalized_choices = {normalize(choice): choice for choice in choices}

    # Perform fuzzy matching
    result = process.extractOne(
        normalized_query,
        normalized_choices.keys(),
        scorer=fuzz.token_set_ratio,
        score_cutoff=score_cutoff
    )

    if result is None:
        return None, 0.0

    normalized_match, score, _ = result
    original_match = normalized_choices[normalized_match]

    return original_match, score
