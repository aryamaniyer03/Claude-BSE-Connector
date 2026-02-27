"""Extended Yahoo Finance data modules for Indian stocks.

Covers: Balance Sheet, Cash Flow, Annual Financials, Historical Prices,
Holders/Ownership, Upgrades/Downgrades, Earnings History, Key Metrics, News.
"""

import logging
from typing import Any

from .yf_utils import (
    resolve_yf_ticker,
    safe_float,
    df_to_records,
    holder_df_to_records,
    CR,
)

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Balance Sheet (quarterly + annual)
# ---------------------------------------------------------------------------

def get_balance_sheet(symbol: str, scrip_code: str = "", quarterly: bool = True) -> dict[str, Any]:
    """Fetch balance sheet data from Yahoo Finance.

    Returns structured balance sheet with all available line items
    (Total Assets, Total Liabilities, Stockholders Equity, Cash, Debt, etc.)
    """
    ticker = resolve_yf_ticker(symbol, scrip_code, validate="info")
    if not ticker:
        return {"error": f"Could not find {symbol} on Yahoo Finance", "data": []}

    try:
        if quarterly:
            bs = ticker.quarterly_balance_sheet
        else:
            bs = ticker.balance_sheet

        if bs is None or bs.empty:
            return {"error": f"No balance sheet data for {symbol}", "symbol": ticker.ticker, "data": []}

        records = df_to_records(bs, convert_cr=True)

        return {
            "symbol": ticker.ticker,
            "type": "quarterly" if quarterly else "annual",
            "num_periods": len(records),
            "data": records,
            "note": "All values in Cr (crores).",
        }
    except Exception as e:
        return {"error": f"Failed to fetch balance sheet: {str(e)}", "data": []}


# ---------------------------------------------------------------------------
# Cash Flow Statement (quarterly + annual)
# ---------------------------------------------------------------------------

def get_cash_flow(symbol: str, scrip_code: str = "", quarterly: bool = True) -> dict[str, Any]:
    """Fetch cash flow statement from Yahoo Finance.

    Returns structured cash flow data with all available line items
    (Operating CF, Investing CF, Financing CF, Free Cash Flow, CapEx, etc.)
    """
    ticker = resolve_yf_ticker(symbol, scrip_code, validate="info")
    if not ticker:
        return {"error": f"Could not find {symbol} on Yahoo Finance", "data": []}

    try:
        if quarterly:
            cf = ticker.quarterly_cash_flow
        else:
            cf = ticker.cash_flow

        if cf is None or cf.empty:
            return {"error": f"No cash flow data for {symbol}", "symbol": ticker.ticker, "data": []}

        records = df_to_records(cf, convert_cr=True)

        return {
            "symbol": ticker.ticker,
            "type": "quarterly" if quarterly else "annual",
            "num_periods": len(records),
            "data": records,
            "note": "All values in Cr (crores).",
        }
    except Exception as e:
        return {"error": f"Failed to fetch cash flow: {str(e)}", "data": []}


# ---------------------------------------------------------------------------
# Annual Income Statement
# ---------------------------------------------------------------------------

def get_annual_financials(symbol: str, scrip_code: str = "") -> dict[str, Any]:
    """Fetch annual income statement from Yahoo Finance.

    Returns structured P&L data with all available line items.
    """
    ticker = resolve_yf_ticker(symbol, scrip_code, validate="info")
    if not ticker:
        return {"error": f"Could not find {symbol} on Yahoo Finance", "data": []}

    try:
        inc = ticker.income_stmt
        if inc is None or inc.empty:
            return {"error": f"No annual income data for {symbol}", "symbol": ticker.ticker, "data": []}

        records = df_to_records(inc, convert_cr=True)

        return {
            "symbol": ticker.ticker,
            "type": "annual",
            "num_periods": len(records),
            "data": records,
            "note": "All values in Cr (crores).",
        }
    except Exception as e:
        return {"error": f"Failed to fetch annual financials: {str(e)}", "data": []}


# ---------------------------------------------------------------------------
# Historical Prices
# ---------------------------------------------------------------------------

def get_price_history(
    symbol: str,
    scrip_code: str = "",
    period: str = "1y",
    interval: str = "1d",
) -> dict[str, Any]:
    """Fetch historical OHLCV price data from Yahoo Finance.

    Args:
        symbol: BSE/NSE symbol
        scrip_code: Optional BSE scrip code
        period: Data period - 1d, 5d, 1mo, 3mo, 6mo, 1y, 2y, 5y, 10y, ytd, max
        interval: Bar interval - 1d, 1wk, 1mo (intraday not reliable for .NS/.BO)

    Returns:
        Dict with OHLCV data points.
    """
    # Validate period and interval
    valid_periods = {"1d", "5d", "1mo", "3mo", "6mo", "1y", "2y", "5y", "10y", "ytd", "max"}
    valid_intervals = {"1d", "5d", "1wk", "1mo", "3mo"}

    if period not in valid_periods:
        period = "1y"
    if interval not in valid_intervals:
        interval = "1d"

    ticker = resolve_yf_ticker(symbol, scrip_code, validate="none")
    if not ticker:
        return {"error": f"Could not find {symbol} on Yahoo Finance", "data": []}

    try:
        hist = ticker.history(period=period, interval=interval)

        if hist is None or hist.empty:
            return {"error": f"No price history for {symbol}", "symbol": ticker.ticker, "data": []}

        records = []
        for idx, row in hist.iterrows():
            date_str = idx.strftime("%Y-%m-%d") if hasattr(idx, "strftime") else str(idx)
            entry = {
                "date": date_str,
                "open": round(float(row.get("Open", 0)), 2),
                "high": round(float(row.get("High", 0)), 2),
                "low": round(float(row.get("Low", 0)), 2),
                "close": round(float(row.get("Close", 0)), 2),
                "volume": int(row.get("Volume", 0)),
            }
            # Include dividends/splits if non-zero
            div = float(row.get("Dividends", 0))
            split = float(row.get("Stock Splits", 0))
            if div:
                entry["dividend"] = round(div, 2)
            if split:
                entry["stock_split"] = round(split, 4)
            records.append(entry)

        return {
            "symbol": ticker.ticker,
            "period": period,
            "interval": interval,
            "num_points": len(records),
            "data": records,
        }
    except Exception as e:
        return {"error": f"Failed to fetch price history: {str(e)}", "data": []}


# ---------------------------------------------------------------------------
# Holders / Ownership
# ---------------------------------------------------------------------------

def get_holders(symbol: str, scrip_code: str = "") -> dict[str, Any]:
    """Fetch shareholding data from Yahoo Finance.

    Returns institutional holders, mutual fund holders, major holders summary,
    and insider transactions.
    """
    ticker = resolve_yf_ticker(symbol, scrip_code, validate="info")
    if not ticker:
        return {"error": f"Could not find {symbol} on Yahoo Finance"}

    result: dict[str, Any] = {"symbol": ticker.ticker}

    # Major holders summary (% insiders, % institutions, etc.)
    try:
        mh = ticker.major_holders
        if mh is not None and not mh.empty:
            major = {}
            for _, row in mh.iterrows():
                val = row.iloc[0] if len(row) > 0 else None
                label = row.iloc[1] if len(row) > 1 else str(row.name)
                if val is not None:
                    try:
                        major[str(label)] = round(float(val), 4)
                    except (ValueError, TypeError):
                        major[str(label)] = str(val)
            if major:
                result["major_holders"] = major
    except Exception:
        pass

    # Institutional holders
    try:
        ih = ticker.institutional_holders
        records = holder_df_to_records(ih)
        if records:
            result["institutional_holders"] = records[:20]  # Top 20
    except Exception:
        pass

    # Mutual fund holders
    try:
        mf = ticker.mutualfund_holders
        records = holder_df_to_records(mf)
        if records:
            result["mutualfund_holders"] = records[:20]
    except Exception:
        pass

    # Insider transactions
    try:
        it = ticker.insider_transactions
        records = holder_df_to_records(it)
        if records:
            result["insider_transactions"] = records[:30]
    except Exception:
        pass

    # Insider purchases summary
    try:
        ip = ticker.insider_purchases
        records = holder_df_to_records(ip)
        if records:
            result["insider_purchases"] = records
    except Exception:
        pass

    if len(result) <= 1:
        return {"error": f"No holder data available for {symbol}", "symbol": ticker.ticker}

    return result


# ---------------------------------------------------------------------------
# Upgrades / Downgrades
# ---------------------------------------------------------------------------

def get_upgrades_downgrades(symbol: str, scrip_code: str = "") -> dict[str, Any]:
    """Fetch analyst upgrades and downgrades from Yahoo Finance.

    Returns a list of rating changes by brokerage firms.
    """
    ticker = resolve_yf_ticker(symbol, scrip_code, validate="info")
    if not ticker:
        return {"error": f"Could not find {symbol} on Yahoo Finance", "data": []}

    try:
        ud = ticker.upgrades_downgrades
        if ud is None or ud.empty:
            return {"error": f"No upgrades/downgrades data for {symbol}", "symbol": ticker.ticker, "data": []}

        records = []
        for idx, row in ud.head(50).iterrows():  # Last 50 rating changes
            date_str = idx.strftime("%Y-%m-%d") if hasattr(idx, "strftime") else str(idx)
            records.append({
                "date": date_str,
                "firm": row.get("Firm", ""),
                "to_grade": row.get("ToGrade", ""),
                "from_grade": row.get("FromGrade", ""),
                "action": row.get("Action", ""),
            })

        return {
            "symbol": ticker.ticker,
            "num_changes": len(records),
            "data": records,
        }
    except Exception as e:
        return {"error": f"Failed to fetch upgrades/downgrades: {str(e)}", "data": []}


# ---------------------------------------------------------------------------
# Earnings History (beat/miss)
# ---------------------------------------------------------------------------

def get_earnings_history(symbol: str, scrip_code: str = "") -> dict[str, Any]:
    """Fetch earnings history — actual vs estimate EPS.

    Shows past earnings announcements with surprise (beat/miss) data.
    """
    ticker = resolve_yf_ticker(symbol, scrip_code, validate="info")
    if not ticker:
        return {"error": f"Could not find {symbol} on Yahoo Finance", "data": []}

    try:
        eh = ticker.earnings_history
        if eh is not None and not eh.empty:
            records = []
            for _, row in eh.iterrows():
                entry = {}
                for col in eh.columns:
                    val = row[col]
                    if val is None or (isinstance(val, float) and val != val):
                        continue
                    if hasattr(val, "isoformat"):
                        val = val.isoformat()
                    elif isinstance(val, float):
                        val = round(val, 4)
                    entry[str(col)] = val
                records.append(entry)

            if records:
                return {
                    "symbol": ticker.ticker,
                    "num_quarters": len(records),
                    "data": records,
                }
    except Exception:
        pass

    # Fallback: use earnings_dates which has similar data
    try:
        ed = ticker.earnings_dates
        if ed is not None and not ed.empty:
            records = []
            for idx, row in ed.head(20).iterrows():
                date_str = idx.strftime("%Y-%m-%d") if hasattr(idx, "strftime") else str(idx)
                entry = {"date": date_str}
                for col in ed.columns:
                    val = row[col]
                    if val is None or (isinstance(val, float) and val != val):
                        continue
                    if isinstance(val, float):
                        val = round(val, 4)
                    entry[str(col)] = val
                records.append(entry)

            return {
                "symbol": ticker.ticker,
                "num_entries": len(records),
                "data": records,
            }
    except Exception as e:
        return {"error": f"Failed to fetch earnings history: {str(e)}", "data": []}

    return {"error": f"No earnings history for {symbol}", "data": []}


# ---------------------------------------------------------------------------
# Key Metrics / Company Profile
# ---------------------------------------------------------------------------

def get_key_metrics(symbol: str, scrip_code: str = "") -> dict[str, Any]:
    """Fetch comprehensive key metrics and company profile from Yahoo Finance.

    Returns: valuation ratios, profitability metrics, balance sheet metrics,
    growth rates, dividend info, short interest, and company description.
    """
    ticker = resolve_yf_ticker(symbol, scrip_code, validate="info")
    if not ticker:
        return {"error": f"Could not find {symbol} on Yahoo Finance"}

    try:
        info = ticker.info
        if not info:
            return {"error": f"No info available for {symbol}"}
    except Exception as e:
        return {"error": f"Failed to fetch info: {str(e)}"}

    result: dict[str, Any] = {"symbol": ticker.ticker}

    # Company profile
    profile = {}
    profile_fields = [
        "longName", "shortName", "sector", "industry", "longBusinessSummary",
        "website", "fullTimeEmployees", "country", "city",
    ]
    for f in profile_fields:
        v = info.get(f)
        if v is not None:
            profile[f] = v
    if profile:
        result["profile"] = profile

    # Price & market data
    market = {}
    market_fields = [
        "currentPrice", "previousClose", "open", "dayLow", "dayHigh",
        "fiftyTwoWeekLow", "fiftyTwoWeekHigh",
        "fiftyDayAverage", "twoHundredDayAverage",
        "volume", "averageVolume", "averageVolume10days",
        "marketCap", "enterpriseValue",
        "sharesOutstanding", "floatShares",
        "beta",
    ]
    for f in market_fields:
        v = info.get(f)
        if v is not None:
            market[f] = round(v, 2) if isinstance(v, float) else v
    if market:
        result["market_data"] = market

    # Valuation ratios
    valuation = {}
    val_fields = [
        "trailingPE", "forwardPE", "priceToBook", "priceToSalesTrailing12Months",
        "enterpriseToRevenue", "enterpriseToEbitda",
        "pegRatio", "trailingPegRatio", "bookValue",
    ]
    for f in val_fields:
        v = info.get(f)
        if v is not None:
            valuation[f] = round(v, 2) if isinstance(v, float) else v
    if valuation:
        result["valuation"] = valuation

    # Profitability
    profitability = {}
    profit_fields = [
        "profitMargins", "grossMargins", "operatingMargins", "ebitdaMargins",
        "returnOnAssets", "returnOnEquity",
        "revenueGrowth", "earningsGrowth", "earningsQuarterlyGrowth",
    ]
    for f in profit_fields:
        v = info.get(f)
        if v is not None:
            profitability[f] = round(v, 4) if isinstance(v, float) else v
    if profitability:
        result["profitability"] = profitability

    # Income / Revenue metrics
    income = {}
    income_fields = [
        "totalRevenue", "ebitda", "netIncomeToCommon", "grossProfits",
        "revenuePerShare", "trailingEps", "forwardEps",
    ]
    for f in income_fields:
        v = info.get(f)
        if v is not None:
            if f in ("totalRevenue", "ebitda", "netIncomeToCommon", "grossProfits"):
                income[f] = round(v / CR, 2)  # Convert to Crores
            else:
                income[f] = round(v, 2) if isinstance(v, float) else v
    if income:
        result["income_metrics"] = income

    # Balance sheet metrics
    balance = {}
    balance_fields = [
        "totalCash", "totalCashPerShare", "totalDebt",
        "debtToEquity", "currentRatio", "quickRatio",
        "operatingCashflow", "freeCashflow",
    ]
    for f in balance_fields:
        v = info.get(f)
        if v is not None:
            if f in ("totalCash", "totalDebt", "operatingCashflow", "freeCashflow"):
                balance[f] = round(v / CR, 2)  # Convert to Crores
            else:
                balance[f] = round(v, 2) if isinstance(v, float) else v
    if balance:
        result["balance_sheet_metrics"] = balance

    # Dividend info
    dividend = {}
    div_fields = [
        "dividendRate", "dividendYield", "exDividendDate",
        "payoutRatio", "fiveYearAvgDividendYield",
        "trailingAnnualDividendRate", "trailingAnnualDividendYield",
        "lastDividendValue", "lastDividendDate",
    ]
    for f in div_fields:
        v = info.get(f)
        if v is not None:
            if hasattr(v, "isoformat"):
                dividend[f] = v.isoformat()
            elif isinstance(v, int) and f in ("exDividendDate", "lastDividendDate"):
                # Convert epoch seconds to ISO date
                from datetime import datetime, timezone
                try:
                    dividend[f] = datetime.fromtimestamp(v, tz=timezone.utc).strftime("%Y-%m-%d")
                except Exception:
                    dividend[f] = v
            elif isinstance(v, float):
                dividend[f] = round(v, 4)
            else:
                dividend[f] = v
    if dividend:
        result["dividend"] = dividend

    # Short interest (less relevant for Indian stocks but include if available)
    short = {}
    short_fields = [
        "sharesShort", "shortRatio", "shortPercentOfFloat",
        "heldPercentInsiders", "heldPercentInstitutions",
    ]
    for f in short_fields:
        v = info.get(f)
        if v is not None:
            short[f] = round(v, 4) if isinstance(v, float) else v
    if short:
        result["ownership_short"] = short

    if len(result) <= 1:
        return {"error": f"No metrics available for {symbol}", "symbol": ticker.ticker}

    return result


# ---------------------------------------------------------------------------
# News
# ---------------------------------------------------------------------------

def get_news(symbol: str, scrip_code: str = "") -> dict[str, Any]:
    """Fetch recent news for a company from Yahoo Finance."""
    ticker = resolve_yf_ticker(symbol, scrip_code, validate="none")
    if not ticker:
        return {"error": f"Could not find {symbol} on Yahoo Finance", "articles": []}

    try:
        news = ticker.news
        if not news:
            return {"error": f"No news available for {symbol}", "symbol": ticker.ticker, "articles": []}

        articles = []
        for item in news[:20]:  # Latest 20 articles
            # Handle both old format (flat) and new format (nested under 'content')
            content = item.get("content", item)

            title = content.get("title", item.get("title", ""))
            article = {"title": title}

            # Publisher — new format nests under provider
            provider = content.get("provider", {})
            publisher = provider.get("displayName") if isinstance(provider, dict) else item.get("publisher", "")
            if publisher:
                article["publisher"] = publisher

            # Link — new format uses canonicalUrl
            canonical = content.get("canonicalUrl", {})
            link = canonical.get("url") if isinstance(canonical, dict) else item.get("link", "")
            if link:
                article["link"] = link

            # Content type
            content_type = content.get("contentType", item.get("type", ""))
            if content_type:
                article["type"] = content_type

            # Published date — new format uses pubDate string, old uses epoch
            pub_date = content.get("pubDate")
            if pub_date:
                article["published"] = pub_date
            else:
                ts = item.get("providerPublishTime")
                if ts:
                    from datetime import datetime, timezone
                    try:
                        article["published"] = datetime.fromtimestamp(ts, tz=timezone.utc).isoformat()
                    except Exception:
                        article["published"] = str(ts)

            # Summary if available
            summary = content.get("summary", "")
            if summary:
                article["summary"] = summary[:300]  # Truncate long summaries

            related = item.get("relatedTickers", [])
            if related:
                article["related_tickers"] = related

            articles.append(article)

        return {
            "symbol": ticker.ticker,
            "num_articles": len(articles),
            "articles": articles,
        }
    except Exception as e:
        return {"error": f"Failed to fetch news: {str(e)}", "articles": []}


# ---------------------------------------------------------------------------
# Sustainability / ESG
# ---------------------------------------------------------------------------

def get_sustainability(symbol: str, scrip_code: str = "") -> dict[str, Any]:
    """Fetch ESG/sustainability scores from Yahoo Finance."""
    ticker = resolve_yf_ticker(symbol, scrip_code, validate="info")
    if not ticker:
        return {"error": f"Could not find {symbol} on Yahoo Finance"}

    try:
        esg = ticker.sustainability
        if esg is None or esg.empty:
            return {"error": f"No ESG data available for {symbol}", "symbol": ticker.ticker}

        data = {}
        for idx in esg.index:
            val = esg.at[idx, esg.columns[0]] if len(esg.columns) > 0 else None
            if val is not None and str(val) != "nan":
                if isinstance(val, float):
                    data[str(idx)] = round(val, 2)
                elif isinstance(val, bool):
                    data[str(idx)] = val
                else:
                    data[str(idx)] = str(val)

        if not data:
            return {"error": f"No ESG data available for {symbol}", "symbol": ticker.ticker}

        return {
            "symbol": ticker.ticker,
            "esg_scores": data,
        }
    except Exception as e:
        return {"error": f"Failed to fetch ESG data: {str(e)}"}


# ---------------------------------------------------------------------------
# Dividends & Splits History
# ---------------------------------------------------------------------------

def get_dividends_splits(symbol: str, scrip_code: str = "") -> dict[str, Any]:
    """Fetch historical dividends and stock splits from Yahoo Finance."""
    ticker = resolve_yf_ticker(symbol, scrip_code, validate="none")
    if not ticker:
        return {"error": f"Could not find {symbol} on Yahoo Finance"}

    result: dict[str, Any] = {"symbol": ticker.ticker}

    # Dividends
    try:
        divs = ticker.dividends
        if divs is not None and not divs.empty:
            div_records = []
            for dt, val in divs.items():
                div_records.append({
                    "date": dt.strftime("%Y-%m-%d") if hasattr(dt, "strftime") else str(dt),
                    "amount": round(float(val), 2),
                })
            result["dividends"] = div_records
            result["num_dividends"] = len(div_records)
    except Exception:
        pass

    # Splits
    try:
        splits = ticker.splits
        if splits is not None and not splits.empty:
            split_records = []
            for dt, val in splits.items():
                split_records.append({
                    "date": dt.strftime("%Y-%m-%d") if hasattr(dt, "strftime") else str(dt),
                    "ratio": round(float(val), 4),
                })
            result["splits"] = split_records
            result["num_splits"] = len(split_records)
    except Exception:
        pass

    if len(result) <= 1:
        return {"error": f"No dividend/split history for {symbol}", "symbol": ticker.ticker}

    return result


# ---------------------------------------------------------------------------
# Shares Outstanding History
# ---------------------------------------------------------------------------

def get_shares_outstanding(symbol: str, scrip_code: str = "") -> dict[str, Any]:
    """Fetch historical shares outstanding data."""
    ticker = resolve_yf_ticker(symbol, scrip_code, validate="info")
    if not ticker:
        return {"error": f"Could not find {symbol} on Yahoo Finance"}

    try:
        shares = ticker.get_shares_full()
        if shares is None or shares.empty:
            return {"error": f"No shares data for {symbol}", "symbol": ticker.ticker}

        records = []
        for dt, val in shares.items():
            records.append({
                "date": dt.strftime("%Y-%m-%d") if hasattr(dt, "strftime") else str(dt),
                "shares": int(val) if val == val else 0,
            })

        return {
            "symbol": ticker.ticker,
            "num_entries": len(records),
            "data": records[-50:],  # Last 50 data points to keep response size manageable
        }
    except Exception as e:
        return {"error": f"Failed to fetch shares data: {str(e)}"}
