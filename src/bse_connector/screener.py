"""Screener.in quarterly P&L scraper — structured financial data for Indian companies."""

import logging
import time
from typing import Any

import httpx
from bs4 import BeautifulSoup

logger = logging.getLogger(__name__)

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
}

# Timeout config: connect fast, read can be slow
TIMEOUT = httpx.Timeout(connect=10.0, read=30.0, write=10.0, pool=10.0)
MAX_RETRIES = 3
RETRY_BACKOFF = [2.0, 4.0, 8.0]  # seconds between retries
MIN_REQUEST_GAP = 1.5  # minimum seconds between requests to screener.in

# Module-level timestamp of last request for rate limiting
_last_request_time: float = 0.0


def _rate_limit():
    """Enforce minimum gap between requests to avoid getting blocked."""
    global _last_request_time
    now = time.monotonic()
    elapsed = now - _last_request_time
    if _last_request_time > 0 and elapsed < MIN_REQUEST_GAP:
        sleep_for = MIN_REQUEST_GAP - elapsed
        logger.debug(f"Rate limiting: sleeping {sleep_for:.1f}s")
        time.sleep(sleep_for)
    _last_request_time = time.monotonic()


def _parse_value(val: str) -> float:
    """Parse a screener value string to a number."""
    if not val or val.strip() == "":
        return 0.0
    cleaned = val.replace(",", "").replace("%", "").strip()
    try:
        return float(cleaned)
    except ValueError:
        return 0.0


def _try_fetch(url: str, client: httpx.Client) -> dict[str, Any] | None:
    """
    Fetch and parse a screener.in page for quarterly data.
    Retries on timeout/connection errors with exponential backoff.
    """
    last_error = None

    for attempt in range(MAX_RETRIES):
        try:
            _rate_limit()
            logger.info(f"Fetching {url} (attempt {attempt + 1}/{MAX_RETRIES})")
            resp = client.get(url)

            if resp.status_code == 404:
                return None
            if resp.status_code == 429:
                # Rate limited — back off longer
                wait = RETRY_BACKOFF[attempt] * 2 if attempt < len(RETRY_BACKOFF) else 16.0
                logger.warning(f"Rate limited (429), waiting {wait:.0f}s")
                time.sleep(wait)
                continue
            resp.raise_for_status()

            return _parse_html(resp.text)

        except (httpx.TimeoutException, httpx.ConnectError, httpx.ReadError) as e:
            last_error = e
            if attempt < MAX_RETRIES - 1:
                wait = RETRY_BACKOFF[attempt]
                logger.warning(f"Request failed ({type(e).__name__}), retrying in {wait:.0f}s")
                time.sleep(wait)
            else:
                logger.warning(f"Request failed after {MAX_RETRIES} attempts: {e}")

        except httpx.HTTPStatusError as e:
            if e.response.status_code == 404:
                return None
            logger.warning(f"HTTP error fetching {url}: {e}")
            return None

        except Exception as e:
            logger.warning(f"Unexpected error fetching {url}: {e}")
            return None

    return None


def _parse_html(html: str) -> dict[str, Any] | None:
    """Parse the quarters section from screener HTML."""
    soup = BeautifulSoup(html, "html.parser")
    section = soup.find("section", id="quarters")
    if not section:
        return None

    table = section.find("table")
    if not table:
        return None

    # Extract date headers
    headers = []
    thead = table.find("thead")
    if thead:
        for th in thead.find_all("th"):
            headers.append(th.get_text(strip=True))

    # Extract data rows
    rows: dict[str, list[str]] = {}
    tbody = table.find("tbody")
    if not tbody:
        return None

    for tr in tbody.find_all("tr"):
        cells = tr.find_all(["th", "td"])
        if not cells:
            continue

        row_name = cells[0].get_text(strip=True)
        row_name = row_name.replace("+", "").strip()

        values = [cell.get_text(strip=True) for cell in cells[1:]]
        rows[row_name] = values

    if not rows:
        return None

    logger.info(f"Got {len(headers) - 1} quarters, {len(rows)} rows")
    return {"headers": headers, "rows": rows}


def get_financials(symbol: str) -> dict[str, Any]:
    """
    Fetch quarterly financial data from screener.in.

    Uses a single httpx.Client for connection reuse, with retry logic
    and rate limiting to handle screener.in's tendency to timeout
    under consecutive calls.

    Args:
        symbol: BSE/NSE symbol or scrip code to look up on screener.in

    Returns:
        Dict with quarters list, each containing structured P&L data.
        Includes EBITDA and EBITDA margin computation.
    """
    with httpx.Client(
        headers=HEADERS,
        timeout=TIMEOUT,
        follow_redirects=True,
        http2=False,
    ) as client:
        # Try consolidated first, then standalone
        data = _try_fetch(f"https://www.screener.in/company/{symbol}/consolidated/", client)

        if not data:
            data = _try_fetch(f"https://www.screener.in/company/{symbol}/", client)

    if not data:
        return {"error": f"Could not fetch quarterly data for {symbol}", "quarters": []}

    headers = data["headers"]
    rows = data["rows"]

    # Map screener row names to our field names
    field_map = {
        "Sales": "revenue",
        "Revenue": "revenue",
        "Expenses": "expenses",
        "Operating Profit": "operating_profit",
        "OPM": "opm_pct",
        "Other Income": "other_income",
        "Interest": "interest",
        "Depreciation": "depreciation",
        "Profit before tax": "pbt",
        "Tax": "tax_pct",
        "Tax %": "tax_pct",
        "Net Profit": "net_profit",
        "EPS in Rs": "eps",
        "EPS": "eps",
    }

    # Build quarters (skip first header which is the row label column)
    quarters = []
    num_quarters = len(headers) - 1

    for i in range(num_quarters):
        quarter: dict[str, Any] = {"period": headers[i + 1]}

        for row_name, values in rows.items():
            field = field_map.get(row_name)
            if field and i < len(values):
                quarter[field] = _parse_value(values[i])

        # Compute EBITDA = Operating Profit + Depreciation
        op_profit = quarter.get("operating_profit", 0.0)
        depreciation = quarter.get("depreciation", 0.0)
        ebitda = op_profit + depreciation
        quarter["ebitda"] = ebitda

        # EBITDA Margin = EBITDA / Revenue * 100
        revenue = quarter.get("revenue", 0.0)
        if revenue > 0:
            quarter["ebitda_margin_pct"] = round(ebitda / revenue * 100, 1)
        else:
            quarter["ebitda_margin_pct"] = 0.0

        quarters.append(quarter)

    return {
        "symbol": symbol,
        "num_quarters": len(quarters),
        "quarters": quarters,
        "fields": [
            "revenue", "expenses", "operating_profit", "opm_pct",
            "other_income", "interest", "depreciation", "pbt",
            "tax_pct", "net_profit", "eps", "ebitda", "ebitda_margin_pct",
        ],
        "note": "All values in Cr (crores). Periods from screener.in quarterly results.",
    }
