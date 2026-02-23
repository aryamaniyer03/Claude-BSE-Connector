"""BSE API client wrapper with fuzzy resolver and screener integration."""

import io
import re
import tempfile
from datetime import date, datetime, timedelta
from typing import Any
from urllib.request import urlopen, Request
from urllib.error import HTTPError, URLError

from bse import BSE
from dateutil.relativedelta import relativedelta

from .analyst import get_analyst_consensus
from .categories import Category, Purpose, get_category_by_name, get_purpose_by_name
from .resolver import SecurityIndex
from .screener import get_financials

try:
    from pypdf import PdfReader
    PDF_SUPPORT = True
except ImportError:
    PDF_SUPPORT = False

try:
    import pytesseract
    from pdf2image import convert_from_bytes
    OCR_SUPPORT = True
except ImportError:
    OCR_SUPPORT = False


# BSE attachment URL paths
BSE_ATTACH_LIVE = "https://www.bseindia.com/xml-data/corpfiling/AttachLive/"
BSE_ATTACH_HIS = "https://www.bseindia.com/xml-data/corpfiling/AttachHis/"

# Shared resolver instance
_resolver = SecurityIndex()


def check_url_exists(url: str, timeout: float = 3.0) -> bool:
    """Check if URL returns 200 (HEAD request)."""
    try:
        req = Request(url, method="HEAD")
        req.add_header("User-Agent", "Mozilla/5.0")
        with urlopen(req, timeout=timeout) as response:
            return response.status == 200
    except (HTTPError, URLError, TimeoutError):
        return False


def get_attachment_url(attachment: str, ann_date: str | None = None) -> str:
    """Get the working attachment URL (AttachLive or AttachHis)."""
    if not attachment:
        return ""

    live_url = f"{BSE_ATTACH_LIVE}{attachment}"
    hist_url = f"{BSE_ATTACH_HIS}{attachment}"

    try_live_first = True
    if ann_date:
        try:
            if "T" in ann_date:
                ann_dt = datetime.fromisoformat(ann_date.split(".")[0]).date()
            else:
                ann_dt = datetime.strptime(ann_date[:10], "%Y-%m-%d").date()
            if (date.today() - ann_dt).days > 7:
                try_live_first = False
        except (ValueError, TypeError):
            pass

    if try_live_first:
        if check_url_exists(live_url):
            return live_url
        if check_url_exists(hist_url):
            return hist_url
    else:
        if check_url_exists(hist_url):
            return hist_url
        if check_url_exists(live_url):
            return live_url

    return live_url


def fetch_pdf_text(url: str, max_pages: int = 50, timeout: float = 30.0, use_ocr: bool = True) -> dict[str, Any]:
    """Fetch PDF from URL and extract text content."""
    if not PDF_SUPPORT:
        return {"text": "", "pages": 0, "error": "PDF support not available (install pypdf)"}

    if not url:
        return {"text": "", "pages": 0, "error": "No URL provided"}

    try:
        req = Request(url)
        req.add_header("User-Agent", "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36")
        req.add_header("Accept", "application/pdf")

        with urlopen(req, timeout=timeout) as response:
            pdf_data = response.read()

        pdf_file = io.BytesIO(pdf_data)
        reader = PdfReader(pdf_file)

        text_parts = []
        num_pages = min(len(reader.pages), max_pages)
        ocr_used = False
        pages_with_text = 0

        for i in range(num_pages):
            page_text = reader.pages[i].extract_text()
            if page_text and len(page_text.strip()) > 50:
                text_parts.append(f"--- Page {i+1} ---\n{page_text}")
                pages_with_text += 1
            else:
                text_parts.append(f"--- Page {i+1} ---\n[SCANNED_IMAGE]")

        scanned_pages = num_pages - pages_with_text
        if use_ocr and OCR_SUPPORT and scanned_pages > num_pages * 0.5:
            try:
                images = convert_from_bytes(pdf_data, first_page=1, last_page=num_pages, dpi=150)
                text_parts = []

                for i, img in enumerate(images):
                    ocr_text = pytesseract.image_to_string(img, lang='eng')
                    if ocr_text and len(ocr_text.strip()) > 20:
                        text_parts.append(f"--- Page {i+1} (OCR) ---\n{ocr_text}")
                    else:
                        text_parts.append(f"--- Page {i+1} ---\n[No text extracted]")

                ocr_used = True
            except Exception:
                pass

        full_text = "\n\n".join(text_parts)

        return {
            "text": full_text,
            "pages": num_pages,
            "total_pages": len(reader.pages),
            "ocr_used": ocr_used,
            "error": None,
        }

    except HTTPError as e:
        return {"text": "", "pages": 0, "error": f"HTTP error {e.code}: {e.reason}"}
    except URLError as e:
        return {"text": "", "pages": 0, "error": f"URL error: {e.reason}"}
    except Exception as e:
        return {"text": "", "pages": 0, "error": f"Error: {str(e)}"}


class BSEClient:
    """Enhanced BSE API client with fuzzy resolver and screener integration."""

    def __init__(self, download_folder: str | None = None):
        self._bse: BSE | None = None
        self._download_folder = download_folder or tempfile.gettempdir()

    def __enter__(self):
        self._bse = BSE(download_folder=self._download_folder)
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if self._bse:
            self._bse.exit()

    @property
    def bse(self) -> BSE:
        if self._bse is None:
            raise RuntimeError("BSEClient must be used as context manager")
        return self._bse

    # -------------------------------------------------------------------------
    # Date utilities
    # -------------------------------------------------------------------------

    @staticmethod
    def parse_quarter(quarter_str: str) -> tuple[date, date] | None:
        """Parse quarter string like 'Q3 2025', 'Q1 FY25' into date range."""
        quarter_str = quarter_str.strip().upper()

        patterns = [
            r"Q([1-4])\s*(?:FY)?(\d{2,4})",
            r"([1-4])Q\s*(?:FY)?(\d{2,4})",
            r"Q([1-4])\s*(\d{2,4})",
        ]

        quarter = None
        year = None
        is_fy = "FY" in quarter_str

        for pattern in patterns:
            match = re.search(pattern, quarter_str)
            if match:
                quarter = int(match.group(1))
                year_str = match.group(2)
                year = int(year_str) if len(year_str) == 4 else 2000 + int(year_str)
                break

        if quarter is None or year is None:
            return None

        if is_fy:
            if quarter == 1:
                start = date(year - 1, 4, 1)
                end = date(year - 1, 6, 30)
            elif quarter == 2:
                start = date(year - 1, 7, 1)
                end = date(year - 1, 9, 30)
            elif quarter == 3:
                start = date(year - 1, 10, 1)
                end = date(year - 1, 12, 31)
            else:
                start = date(year, 1, 1)
                end = date(year, 3, 31)
        else:
            if quarter == 1:
                start = date(year, 1, 1)
                end = date(year, 3, 31)
            elif quarter == 2:
                start = date(year, 4, 1)
                end = date(year, 6, 30)
            elif quarter == 3:
                start = date(year, 7, 1)
                end = date(year, 9, 30)
            else:
                start = date(year, 10, 1)
                end = date(year, 12, 31)

        return (start, end)

    @staticmethod
    def parse_date_input(
        date_input: str | None, default_days_back: int = 90
    ) -> tuple[date, date]:
        """Parse flexible date input into (from_date, to_date)."""
        today = date.today()

        if date_input is None:
            return (today - timedelta(days=default_days_back), today)

        date_input = date_input.strip()

        quarter_range = BSEClient.parse_quarter(date_input)
        if quarter_range:
            return quarter_range

        if re.match(r"^\d{4}$", date_input):
            year = int(date_input)
            return (date(year, 1, 1), date(year, 12, 31))

        days_match = re.search(r"(?:last|past)\s*(\d+)\s*days?", date_input.lower())
        if days_match:
            days = int(days_match.group(1))
            return (today - timedelta(days=days), today)

        months_match = re.search(r"(?:last|past)\s*(\d+)\s*months?", date_input.lower())
        if months_match:
            months = int(months_match.group(1))
            return (today - relativedelta(months=months), today)

        for fmt in ["%Y-%m-%d", "%d-%m-%Y", "%d/%m/%Y", "%Y/%m/%d"]:
            try:
                parsed = datetime.strptime(date_input, fmt).date()
                return (parsed, today)
            except ValueError:
                continue

        return (today - timedelta(days=default_days_back), today)

    @staticmethod
    def split_date_range(
        from_date: date, to_date: date, max_days: int = 30
    ) -> list[tuple[date, date]]:
        """Split date range into chunks of max_days for BSE API limits."""
        chunks = []
        current = from_date

        while current < to_date:
            chunk_end = min(current + timedelta(days=max_days - 1), to_date)
            chunks.append((current, chunk_end))
            current = chunk_end + timedelta(days=1)

        return chunks

    # -------------------------------------------------------------------------
    # Company resolution (fuzzy via resolver)
    # -------------------------------------------------------------------------

    def resolve_company(self, query: str) -> dict[str, Any] | None:
        """Resolve company query using fuzzy matching against BSE securities list."""
        query = query.strip()
        if not query:
            return None

        matches = _resolver.resolve(query, bse_instance=self.bse, top_n=1)
        if matches:
            m = matches[0]
            return {
                "scrip_code": m["scrip_code"],
                "name": m["name"],
                "symbol": m["scrip_id"],
                "isin": m.get("isin", ""),
                "group": m.get("group", ""),
                "score": m.get("score", 100),
            }

        # Fallback: try BSE library's getScripCode
        if query.isdigit():
            try:
                name = self.bse.getScripName(query)
                if name:
                    return {"scrip_code": query, "name": name, "symbol": name}
            except Exception:
                pass

        return None

    def search_company(self, query: str, top_n: int = 5) -> list[dict[str, Any]]:
        """Search for companies with fuzzy matching. Returns top N matches with scores."""
        return _resolver.resolve(query, bse_instance=self.bse, top_n=top_n)

    # -------------------------------------------------------------------------
    # Quarterly Financials (NEW)
    # -------------------------------------------------------------------------

    def get_quarterly_financials(self, company: str) -> dict[str, Any]:
        """
        Get structured quarterly P&L data from screener.in.

        Tries BSE scrip_id first, then issuer_name cleaned for screener URL.
        """
        company_info = self.resolve_company(company)

        if not company_info:
            return {"error": f"Company not found: {company}"}

        # Try scrip_id (BSE symbol) on screener
        symbol = company_info.get("symbol", "")
        if symbol:
            result = get_financials(symbol)
            if not result.get("error"):
                result["company"] = company_info
                return result

        # Try scrip code
        code = company_info.get("scrip_code", "")
        if code:
            result = get_financials(code)
            if not result.get("error"):
                result["company"] = company_info
                return result

        # Try cleaned name
        name = company_info.get("name", "")
        if name:
            cleaned = re.sub(r"[^a-zA-Z0-9]", "", name)
            result = get_financials(cleaned)
            if not result.get("error"):
                result["company"] = company_info
                return result

        return {
            "error": f"Could not fetch financials for {company} (tried {symbol}, {code})",
            "company": company_info,
            "quarters": [],
        }

    # -------------------------------------------------------------------------
    # Analyst Consensus (Yahoo Finance)
    # -------------------------------------------------------------------------

    def get_analyst_consensus(self, company: str) -> dict[str, Any]:
        """
        Get analyst consensus estimates from Yahoo Finance.

        Returns target prices, Buy/Hold/Sell ratings, EPS and revenue
        forecasts, EPS trend/revisions, and growth estimates.
        """
        company_info = self.resolve_company(company)

        if not company_info:
            return {"error": f"Company not found: {company}"}

        symbol = company_info.get("symbol", "")
        scrip_code = company_info.get("scrip_code", "")
        name = company_info.get("name", "")

        result = get_analyst_consensus(symbol, scrip_code, name)
        result["company"] = company_info
        return result

    # -------------------------------------------------------------------------
    # Announcements
    # -------------------------------------------------------------------------

    def get_announcements(
        self,
        company: str | None = None,
        category: str | None = None,
        subcategory: str | None = None,
        keyword: str | None = None,
        date_range: str | None = None,
        from_date: date | None = None,
        to_date: date | None = None,
        page: int = 1,
        validate_urls: bool = True,
    ) -> dict[str, Any]:
        """Fetch corporate announcements with flexible filtering."""
        scrip_code = None
        company_info = None
        if company:
            company_info = self.resolve_company(company)
            if company_info:
                scrip_code = company_info["scrip_code"]

        cat_value = "-1"
        if category:
            cat_enum = get_category_by_name(category)
            if cat_enum:
                cat_value = cat_enum.value

        if from_date and to_date:
            start, end = from_date, to_date
        else:
            start, end = self.parse_date_input(date_range)

        try:
            result = self.bse.announcements(
                page_no=page,
                from_date=start,
                to_date=end,
                scripcode=scrip_code,
                category=cat_value,
                subcategory="-1",
            )

            announcements = result.get("Table", [])
            total_info = result.get("Table1", [{}])[0]
            total_count = total_info.get("RowCount", len(announcements))

            formatted = []

            keyword_config = {
                "transcript": {
                    "include": ["transcript", "concall", "con-call", "conference call", "earnings call transcript",
                               "update on institutional", "update on investor", "audio recording"],
                    "exclude": ["intimation", "notice of", "schedule"],
                    "subcategories": ["earnings call transcript"],
                },
                "concall": {
                    "include": ["transcript", "concall", "con-call", "conference call", "earnings call transcript",
                               "update on institutional", "update on investor", "audio recording"],
                    "exclude": ["intimation", "notice of", "schedule"],
                    "subcategories": ["earnings call transcript"],
                },
                "presentation": {
                    "include": ["presentation", "investor presentation", "analyst presentation", "capital market day",
                               "investor meet", "analyst meet"],
                    "exclude": ["intimation", "notice of", "schedule"],
                    "subcategories": ["analyst / investor meet", "investor presentation"],
                },
                "annual report": {
                    "include": ["annual report", "integrated report", "annual return"],
                    "exclude": [],
                    "subcategories": [],
                },
                "investor": {
                    "include": ["investor meet", "institutional investor", "analyst meet", "investor presentation",
                               "update on institutional", "update on investor", "capital market day"],
                    "exclude": ["intimation", "notice of", "schedule"],
                    "subcategories": ["analyst / investor meet", "investor presentation"],
                },
                "results": {
                    "include": ["financial result", "quarterly result", "audited", "unaudited", "half year", "nine month"],
                    "exclude": [],
                    "subcategories": ["financial results"],
                },
                "agm": {
                    "include": ["agm", "annual general meeting", "egm", "extraordinary general meeting", "postal ballot", "voting result"],
                    "exclude": [],
                    "subcategories": [],
                },
                "press release": {
                    "include": ["press release", "media release", "press conference"],
                    "exclude": ["cancellation"],
                    "subcategories": ["press release / media release"],
                },
                "credit rating": {
                    "include": ["credit rating", "rating revision", "rating reaffirm"],
                    "exclude": [],
                    "subcategories": ["credit rating"],
                },
                "acquisition": {
                    "include": ["acquisition", "acquire", "merger", "amalgamation", "takeover"],
                    "exclude": [],
                    "subcategories": [],
                },
            }

            include_terms = []
            exclude_terms = []
            keyword_subcategories = []
            if keyword:
                keyword_lower = keyword.lower()
                if keyword_lower in keyword_config:
                    include_terms = keyword_config[keyword_lower]["include"]
                    exclude_terms = keyword_config[keyword_lower]["exclude"]
                    keyword_subcategories = keyword_config[keyword_lower].get("subcategories", [])
                else:
                    include_terms = [keyword_lower]

            subcat_filter = subcategory.lower() if subcategory else None

            for ann in announcements:
                headline = ann.get("NEWSSUB") or ann.get("NEWS_SUBJECT") or ""
                headline_lower = headline.lower()
                ann_subcategory = ann.get("SUBCATNAME") or ""
                ann_subcategory_lower = ann_subcategory.lower()

                if subcat_filter and subcat_filter not in ann_subcategory_lower:
                    continue

                if include_terms:
                    headline_match = any(term in headline_lower for term in include_terms)
                    subcategory_match = any(sc in ann_subcategory_lower for sc in keyword_subcategories)

                    if not (headline_match or subcategory_match):
                        continue

                    if not subcategory_match and any(term in headline_lower for term in exclude_terms):
                        continue

                attachment = ann.get("ATTACHMENTNAME", "")
                ann_date = ann.get("NEWS_DT", ann.get("DT_TM", ""))

                if validate_urls and attachment:
                    attachment_url = get_attachment_url(attachment, ann_date)
                elif attachment:
                    attachment_url = f"{BSE_ATTACH_LIVE}{attachment}"
                else:
                    attachment_url = ""

                formatted.append({
                    "headline": headline,
                    "company": ann.get("SLONGNAME", ann.get("COMPANY_NAME", "")),
                    "scrip_code": ann.get("SCRIP_CD", ann.get("SCRIPCODE", "")),
                    "category": ann.get("CATEGORYNAME", ann.get("CATEGORY", "")),
                    "subcategory": ann_subcategory,
                    "date": ann_date,
                    "attachment": attachment,
                    "attachment_url": attachment_url,
                    "news_id": ann.get("NEWSID", ""),
                })

            return {
                "announcements": formatted,
                "total_count": len(formatted) if (keyword or subcat_filter) else total_count,
                "page": page,
                "from_date": start.isoformat(),
                "to_date": end.isoformat(),
                "company": company_info,
                "category_filter": category,
                "subcategory_filter": subcategory,
                "keyword_filter": keyword,
            }

        except Exception as e:
            return {
                "error": str(e),
                "announcements": [],
                "total_count": 0,
                "page": page,
            }

    # -------------------------------------------------------------------------
    # Corporate Actions
    # -------------------------------------------------------------------------

    def get_corporate_actions(
        self,
        company: str | None = None,
        action_type: str | None = None,
        date_range: str | None = None,
        from_date: date | None = None,
        to_date: date | None = None,
    ) -> dict[str, Any]:
        """Fetch corporate actions (dividends, bonuses, splits, etc.)."""
        scrip_code = None
        company_info = None
        if company:
            company_info = self.resolve_company(company)
            if company_info:
                scrip_code = company_info["scrip_code"]

        purpose_code = None
        if action_type and action_type.lower() != "all":
            purpose = get_purpose_by_name(action_type)
            if purpose:
                purpose_code = purpose.value

        if from_date and to_date:
            start, end = from_date, to_date
        else:
            start, end = self.parse_date_input(date_range, default_days_back=365)

        all_actions = []
        chunks = self.split_date_range(start, end, max_days=90)

        for chunk_start, chunk_end in chunks:
            try:
                actions = self.bse.actions(
                    from_date=chunk_start,
                    to_date=chunk_end,
                    scripcode=scrip_code,
                    purpose_code=purpose_code,
                )

                for action in actions:
                    all_actions.append({
                        "company": action.get("long_name", action.get("short_name", "")),
                        "scrip_code": action.get("scrip_code", ""),
                        "symbol": action.get("short_name", ""),
                        "action_type": action.get("Purpose", ""),
                        "ex_date": action.get("Ex_date", action.get("exdate", "")),
                        "record_date": action.get("RD_Date", ""),
                        "bc_start": action.get("BCRD_FROM", ""),
                        "bc_end": action.get("BCRD_TO", ""),
                        "payment_date": action.get("payment_date", ""),
                    })

            except Exception:
                continue

        return {
            "actions": all_actions,
            "total_count": len(all_actions),
            "from_date": start.isoformat(),
            "to_date": end.isoformat(),
            "company": company_info,
            "action_type_filter": action_type,
        }

    # -------------------------------------------------------------------------
    # Result Calendar
    # -------------------------------------------------------------------------

    def get_result_calendar(
        self,
        company: str | None = None,
        date_range: str | None = None,
        from_date: date | None = None,
        to_date: date | None = None,
    ) -> dict[str, Any]:
        """Fetch earnings announcement calendar."""
        scrip_code = None
        company_info = None
        if company:
            company_info = self.resolve_company(company)
            if company_info:
                scrip_code = company_info["scrip_code"]

        if from_date and to_date:
            start, end = from_date, to_date
        else:
            start, end = self.parse_date_input(date_range, default_days_back=30)

        try:
            results = self.bse.resultCalendar(
                from_date=start,
                to_date=end,
                scripcode=scrip_code,
            )

            formatted = []
            for r in results:
                formatted.append({
                    "company": r.get("Long_Name", r.get("long_name", "")),
                    "scrip_code": r.get("scrip_Code", r.get("scripcode", "")),
                    "symbol": r.get("short_name", ""),
                    "result_date": r.get("meeting_date", ""),
                    "bse_url": r.get("URL", ""),
                })

            return {
                "results": formatted,
                "total_count": len(formatted),
                "from_date": start.isoformat(),
                "to_date": end.isoformat(),
                "company": company_info,
            }

        except Exception as e:
            return {
                "error": str(e),
                "results": [],
                "total_count": 0,
            }

    # -------------------------------------------------------------------------
    # Quote / Company Info
    # -------------------------------------------------------------------------

    def get_company_info(self, company: str) -> dict[str, Any]:
        """Get detailed company information and current quote."""
        company_info = self.resolve_company(company)

        if not company_info:
            return {"error": f"Company not found: {company}"}

        scrip_code = company_info["scrip_code"]

        try:
            quote = self.bse.quote(scrip_code)
            company_info["quote"] = {
                "last_price": quote.get("LTP", quote.get("lastTradedPrice", "")),
                "open": quote.get("open", ""),
                "high": quote.get("high", ""),
                "low": quote.get("low", ""),
                "prev_close": quote.get("previousClose", quote.get("prevClose", "")),
                "change": quote.get("change", ""),
                "pct_change": quote.get("pctChange", quote.get("percentChange", "")),
            }
        except Exception:
            pass

        try:
            hl = self.bse.quoteWeeklyHL(scrip_code)
            company_info["week_52"] = {
                "high": hl.get("Fifty2WkHigh_adj", hl.get("high52", "")),
                "low": hl.get("Fifty2WkLow_adj", hl.get("low52", "")),
            }
        except Exception:
            pass

        return company_info

    # -------------------------------------------------------------------------
    # Comprehensive Research (with caching)
    # -------------------------------------------------------------------------

    def research_company(
        self,
        company: str,
        query: str = "",
        focus: str = "all",
        periods: int = 3,
        use_cache: bool = True,
    ) -> dict[str, Any]:
        """Comprehensive company research with smart caching and chunking."""
        from .cache import (
            is_cached, cache_document, get_cached_document,
            get_company_documents, get_relevant_chunks,
        )

        company_info = self.resolve_company(company)
        if not company_info:
            return {"error": f"Company not found: {company}"}

        scrip_code = company_info["scrip_code"]
        periods = min(periods, 5)

        doc_types = []
        if focus in ("all", "guidance"):
            doc_types.extend(["transcript", "presentation"])
        if focus in ("all", "financials"):
            doc_types.extend(["results"])
        if focus == "transcripts":
            doc_types = ["transcript"]
        if focus == "annual":
            doc_types = ["annual report"]

        cached_docs = get_company_documents(scrip_code) if use_cache else []
        cached_urls = {d["url"] for d in cached_docs}

        documents_to_fetch = []

        for doc_type in doc_types:
            result = self.get_announcements(
                company=company,
                keyword=doc_type,
                date_range="last 18 months",
                validate_urls=True,
            )

            count_for_type = 0
            for ann in result.get("announcements", []):
                if count_for_type >= periods:
                    break

                url = ann.get("attachment_url", "")
                headline = ann.get("headline", "").lower()

                if any(skip in headline for skip in [
                    "update on institutional",
                    "update on investor",
                    "intimation of",
                    "notice of",
                    "schedule of",
                ]):
                    continue

                if url:
                    if url in cached_urls:
                        count_for_type += 1
                        continue

                    documents_to_fetch.append({
                        "type": doc_type,
                        "url": url,
                        "headline": ann.get("headline", ""),
                        "date": ann.get("date", "")[:10],
                        "subcat": ann.get("subcategory", ""),
                    })
                    count_for_type += 1

        documents_to_fetch = documents_to_fetch[:5]

        newly_cached = 0
        for doc in documents_to_fetch:
            pdf_result = fetch_pdf_text(doc["url"], max_pages=50)

            if pdf_result.get("text") and not pdf_result.get("error"):
                cache_document(
                    url=doc["url"],
                    company_code=scrip_code,
                    company_name=company_info["name"],
                    doc_type=doc["type"],
                    headline=doc["headline"],
                    date=doc["date"],
                    full_text=pdf_result["text"],
                    pages=pdf_result.get("pages", 0),
                    ocr_used=pdf_result.get("ocr_used", False),
                )
                newly_cached += 1

        chunk_types = None
        if focus == "guidance":
            chunk_types = ["guidance", "summary", "qa"]
        elif focus == "financials":
            chunk_types = ["financials", "summary"]
        elif focus == "transcripts":
            chunk_types = ["qa", "guidance"]

        chunks = get_relevant_chunks(
            company_code=scrip_code,
            query=query or focus,
            chunk_types=chunk_types,
            max_chunks=15,
            max_chars=60000,
        )

        all_cached = get_company_documents(scrip_code)

        return {
            "company": company_info,
            "focus": focus,
            "query": query,
            "documents_cached": len(all_cached),
            "documents_fetched_now": newly_cached,
            "chunks_returned": len(chunks),
            "documents": [
                {
                    "headline": d["headline"],
                    "type": d["doc_type"],
                    "date": d["date"],
                    "cached": True,
                }
                for d in all_cached[:10]
            ],
            "relevant_content": chunks,
        }
