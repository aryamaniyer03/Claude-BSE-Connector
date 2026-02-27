"""Shared Yahoo Finance ticker resolution for Indian stocks."""

import logging
from typing import Any

import yfinance as yf

logger = logging.getLogger(__name__)

# Conversion factor: Yahoo returns absolute INR, we want Crores
CR = 1e7


def resolve_yf_ticker(symbol: str, scrip_code: str = "", validate: str = "info") -> yf.Ticker | None:
    """
    Resolve an Indian stock to a yfinance Ticker object.

    Tries NSE (.NS) first, then BSE (.BO), then scrip code with .BO.

    Args:
        symbol: BSE/NSE symbol (e.g. 'TCS', 'RELIANCE')
        scrip_code: Optional BSE scrip code (e.g. '500325')
        validate: Validation mode - 'info' checks regularMarketPrice,
                  'financials' checks quarterly_income_stmt,
                  'none' skips validation.

    Returns:
        yfinance Ticker or None if not found.
    """
    candidates = []

    if symbol and not symbol.isdigit():
        candidates.append(f"{symbol}.NS")
        candidates.append(f"{symbol}.BO")
    elif symbol and symbol.isdigit():
        candidates.append(f"{symbol}.BO")

    if scrip_code and scrip_code.isdigit():
        candidates.append(f"{scrip_code}.BO")

    for candidate in candidates:
        t = _try_ticker(candidate, validate)
        if t:
            return t

    return None


def _try_ticker(symbol: str, validate: str = "info") -> yf.Ticker | None:
    """Try to create a Ticker and verify it has data."""
    try:
        t = yf.Ticker(symbol)
        if validate == "none":
            return t
        elif validate == "financials":
            qi = t.quarterly_income_stmt
            if qi is not None and not qi.empty and len(qi.columns) > 0:
                return t
        else:  # "info"
            info = t.info
            if info and info.get("regularMarketPrice") is not None:
                return t
    except Exception:
        pass
    return None


def safe_float(val: Any) -> float:
    """Convert a value to float, treating None/NaN as 0."""
    if val is None:
        return 0.0
    try:
        f = float(val)
        return 0.0 if f != f else f  # NaN check
    except (ValueError, TypeError):
        return 0.0


def df_to_records(df, convert_cr: bool = False) -> list[dict[str, Any]]:
    """Convert a pandas DataFrame (financial statement) to a list of dicts.

    Columns are dates (periods), rows are line items.
    Returns one dict per period with all line items.

    Args:
        df: DataFrame from yfinance (columns=dates, rows=line items)
        convert_cr: If True, divide numeric values by CR (1e7) for Crore conversion
    """
    if df is None or df.empty:
        return []

    records = []
    for col in df.columns:
        period = col.strftime("%b %Y") if hasattr(col, "strftime") else str(col)
        entry = {"period": period}
        for item in df.index:
            val = safe_float(df.at[item, col])
            if convert_cr and val != 0:
                val = round(val / CR, 2)
            else:
                val = round(val, 2)
            # Convert item name to snake_case
            key = str(item).replace(" ", "_").replace("/", "_").lower()
            entry[key] = val
        records.append(entry)
    return records


def holder_df_to_records(df) -> list[dict[str, Any]]:
    """Convert a holders DataFrame to a list of dicts."""
    if df is None or df.empty:
        return []

    records = []
    for _, row in df.iterrows():
        entry = {}
        for col in df.columns:
            val = row[col]
            if val is None or (isinstance(val, float) and val != val):
                continue
            if hasattr(val, "isoformat"):
                val = val.isoformat()
            elif isinstance(val, float):
                val = round(val, 4)
            entry[str(col)] = val
        records.append(entry)
    return records
