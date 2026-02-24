"""Quarterly P&L data via Yahoo Finance — structured financial data for Indian companies."""

import logging
from typing import Any

import yfinance as yf

logger = logging.getLogger(__name__)

# Conversion factor: Yahoo returns absolute INR, we want Crores
CR = 1e7


def _safe(val: Any) -> float:
    """Convert a value to float, treating None/NaN as 0."""
    if val is None:
        return 0.0
    try:
        f = float(val)
        return 0.0 if f != f else f  # NaN check
    except (ValueError, TypeError):
        return 0.0


def _try_ticker(symbol: str) -> yf.Ticker | None:
    """Try to create a Ticker and verify it has quarterly data."""
    try:
        t = yf.Ticker(symbol)
        qi = t.quarterly_income_stmt
        if qi is not None and not qi.empty and len(qi.columns) > 0:
            return t
    except Exception:
        pass
    return None


def get_financials(symbol: str) -> dict[str, Any]:
    """
    Fetch quarterly financial data from Yahoo Finance.

    Tries NSE symbol first (.NS), then BSE (.BO).
    Returns structured P&L data matching the screener.in format.

    Args:
        symbol: BSE/NSE symbol or scrip code

    Returns:
        Dict with quarters list, each containing structured P&L data.
    """
    ticker = None

    # Try NSE first (most Indian stocks), then BSE
    if not symbol.isdigit():
        ticker = _try_ticker(f"{symbol}.NS")
        if not ticker:
            ticker = _try_ticker(f"{symbol}.BO")
    else:
        # Scrip code — try BSE
        ticker = _try_ticker(f"{symbol}.BO")

    if not ticker:
        return {"error": f"Could not fetch quarterly data for {symbol}", "quarters": []}

    qi = ticker.quarterly_income_stmt

    # Yahoo returns columns as dates (newest first), rows as line items
    quarters = []
    for col in qi.columns:
        period = col.strftime("%b %Y")  # e.g. "Dec 2025"

        revenue = _safe(qi.at["Total Revenue", col]) if "Total Revenue" in qi.index else 0.0
        expenses = _safe(qi.at["Total Expenses", col]) if "Total Expenses" in qi.index else 0.0
        operating_profit = _safe(qi.at["Operating Income", col]) if "Operating Income" in qi.index else 0.0
        other_income = _safe(qi.at["Other Non Operating Income Expenses", col]) if "Other Non Operating Income Expenses" in qi.index else 0.0
        interest = _safe(qi.at["Interest Expense", col]) if "Interest Expense" in qi.index else 0.0
        depreciation = _safe(qi.at["Depreciation And Amortization In Income Statement", col]) if "Depreciation And Amortization In Income Statement" in qi.index else 0.0
        pbt = _safe(qi.at["Pretax Income", col]) if "Pretax Income" in qi.index else 0.0
        tax_provision = _safe(qi.at["Tax Provision", col]) if "Tax Provision" in qi.index else 0.0
        net_profit = _safe(qi.at["Net Income", col]) if "Net Income" in qi.index else 0.0
        ebitda = _safe(qi.at["EBITDA", col]) if "EBITDA" in qi.index else 0.0
        eps = _safe(qi.at["Diluted EPS", col]) if "Diluted EPS" in qi.index else 0.0

        # Convert from absolute INR to Crores
        rev_cr = round(revenue / CR, 2)
        exp_cr = round(expenses / CR, 2)
        op_cr = round(operating_profit / CR, 2)
        oi_cr = round(other_income / CR, 2)
        int_cr = round(abs(interest) / CR, 2)  # interest is often negative
        dep_cr = round(depreciation / CR, 2)
        pbt_cr = round(pbt / CR, 2)
        np_cr = round(net_profit / CR, 2)
        ebitda_cr = round(ebitda / CR, 2)

        # Compute margins
        opm_pct = round(operating_profit / revenue * 100, 1) if revenue else 0.0
        tax_pct = round(tax_provision / pbt * 100, 1) if pbt else 0.0
        ebitda_margin = round(ebitda / revenue * 100, 1) if revenue else 0.0

        quarters.append({
            "period": period,
            "revenue": rev_cr,
            "expenses": exp_cr,
            "operating_profit": op_cr,
            "opm_pct": opm_pct,
            "other_income": oi_cr,
            "interest": int_cr,
            "depreciation": dep_cr,
            "pbt": pbt_cr,
            "tax_pct": tax_pct,
            "net_profit": np_cr,
            "eps": round(eps, 2),
            "ebitda": ebitda_cr,
            "ebitda_margin_pct": ebitda_margin,
        })

    logger.info(f"Got {len(quarters)} quarters for {ticker.ticker}")

    return {
        "symbol": ticker.ticker,
        "num_quarters": len(quarters),
        "quarters": quarters,
        "fields": [
            "revenue", "expenses", "operating_profit", "opm_pct",
            "other_income", "interest", "depreciation", "pbt",
            "tax_pct", "net_profit", "eps", "ebitda", "ebitda_margin_pct",
        ],
        "note": "All values in Cr (crores). Source: Yahoo Finance.",
    }
