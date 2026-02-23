"""Analyst consensus estimates via Yahoo Finance (yfinance)."""

import logging
from typing import Any

import yfinance as yf

logger = logging.getLogger("bse-connector.analyst")


def _try_ticker(symbol: str) -> yf.Ticker | None:
    """Try to create a Ticker and verify it has data."""
    try:
        t = yf.Ticker(symbol)
        info = t.info
        # Check it resolved to a real stock
        if info and info.get("regularMarketPrice") is not None:
            return t
    except Exception:
        pass
    return None


def get_analyst_consensus(symbol: str, scrip_code: str = "", name: str = "") -> dict[str, Any]:
    """
    Fetch analyst consensus estimates from Yahoo Finance.

    Tries NSE symbol first, then BSE symbol, then name-based search.
    Returns target prices, recommendations, EPS/revenue estimates, and more.
    """
    ticker = None

    # Try NSE symbol (most common for Indian stocks on Yahoo)
    if symbol:
        ticker = _try_ticker(f"{symbol}.NS")
        if not ticker:
            ticker = _try_ticker(f"{symbol}.BO")

    # Try scrip code as BSE ticker
    if not ticker and scrip_code:
        ticker = _try_ticker(f"{scrip_code}.BO")

    if not ticker:
        return {"error": f"Could not find {symbol or name} on Yahoo Finance"}

    result: dict[str, Any] = {"symbol": ticker.ticker}

    # Target prices
    try:
        tp = ticker.analyst_price_targets
        if tp:
            result["target_price"] = {
                "current": tp.get("current"),
                "mean": tp.get("mean"),
                "median": tp.get("median"),
                "high": tp.get("high"),
                "low": tp.get("low"),
            }
    except Exception:
        pass

    # Recommendation summary (Buy/Hold/Sell counts by month)
    try:
        recs = ticker.recommendations_summary
        if recs is not None and not recs.empty:
            result["recommendations"] = []
            for _, row in recs.iterrows():
                result["recommendations"].append({
                    "period": row.get("period", ""),
                    "strong_buy": int(row.get("strongBuy", 0)),
                    "buy": int(row.get("buy", 0)),
                    "hold": int(row.get("hold", 0)),
                    "sell": int(row.get("sell", 0)),
                    "strong_sell": int(row.get("strongSell", 0)),
                })
    except Exception:
        pass

    # Key analyst fields from info
    try:
        info = ticker.info
        analyst_info = {}
        fields = [
            "numberOfAnalystOpinions", "recommendationKey",
            "targetHighPrice", "targetLowPrice", "targetMeanPrice", "targetMedianPrice",
            "forwardPE", "trailingPE", "forwardEps", "trailingEps",
            "earningsGrowth", "earningsQuarterlyGrowth",
            "priceToBook", "enterpriseToEbitda", "enterpriseToRevenue",
            "profitMargins", "returnOnEquity",
        ]
        for f in fields:
            v = info.get(f)
            if v is not None:
                analyst_info[f] = v
        if analyst_info:
            result["info"] = analyst_info
    except Exception:
        pass

    # EPS estimates
    try:
        ee = ticker.earnings_estimate
        if ee is not None and not ee.empty:
            result["eps_estimates"] = []
            for period, row in ee.iterrows():
                entry = {"period": str(period)}
                for col in ee.columns:
                    v = row[col]
                    if v is not None and str(v) != "nan":
                        entry[col] = float(v) if not isinstance(v, str) else v
                if len(entry) > 1:
                    result["eps_estimates"].append(entry)
    except Exception:
        pass

    # Revenue estimates
    try:
        re_ = ticker.revenue_estimate
        if re_ is not None and not re_.empty:
            result["revenue_estimates"] = []
            for period, row in re_.iterrows():
                entry = {"period": str(period)}
                for col in re_.columns:
                    v = row[col]
                    if v is not None and str(v) != "nan":
                        entry[col] = float(v) if not isinstance(v, str) else v
                if len(entry) > 1:
                    result["revenue_estimates"].append(entry)
    except Exception:
        pass

    # EPS trend (how estimates have changed over time)
    try:
        et = ticker.eps_trend
        if et is not None and not et.empty:
            result["eps_trend"] = []
            for period, row in et.iterrows():
                entry = {"period": str(period)}
                for col in et.columns:
                    v = row[col]
                    if v is not None and str(v) != "nan":
                        entry[col] = float(v)
                if len(entry) > 1:
                    result["eps_trend"].append(entry)
    except Exception:
        pass

    # EPS revisions
    try:
        er = ticker.eps_revisions
        if er is not None and not er.empty:
            result["eps_revisions"] = []
            for period, row in er.iterrows():
                entry = {"period": str(period)}
                for col in er.columns:
                    v = row[col]
                    if v is not None and str(v) != "nan":
                        entry[col] = float(v)
                if len(entry) > 1:
                    result["eps_revisions"].append(entry)
    except Exception:
        pass

    # Growth estimates
    try:
        ge = ticker.growth_estimates
        if ge is not None and not ge.empty:
            result["growth_estimates"] = []
            for period, row in ge.iterrows():
                entry = {"period": str(period)}
                for col in ge.columns:
                    v = row[col]
                    if v is not None and str(v) != "nan":
                        entry[col] = float(v)
                if len(entry) > 1:
                    result["growth_estimates"].append(entry)
    except Exception:
        pass

    if len(result) <= 1:
        return {"error": f"No analyst data available for {symbol}", "symbol": ticker.ticker}

    return result
