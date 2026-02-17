"""Fuzzy company resolution using rapidfuzz against BSE securities list."""

import logging
import re
from typing import Any

from rapidfuzz import fuzz, process

from .cache import get_securities_age, load_securities, save_securities, SECURITIES_TTL_HOURS

logger = logging.getLogger(__name__)

# BSE security groups to load
GROUPS = ["A", "B", "T", "X", "XT", "Z", "M", "MT", "P"]

# ISIN pattern: 2 letters + 10 alphanum
ISIN_PATTERN = re.compile(r"^[A-Z]{2}[A-Z0-9]{10}$", re.IGNORECASE)


class SecurityIndex:
    """
    In-memory index of BSE securities with fuzzy matching.

    On first use, fetches all securities via bse.listSecurities() and caches
    in SQLite. Subsequent loads within 24h use the cache.
    """

    def __init__(self):
        self._securities: list[dict] = []
        self._by_code: dict[str, dict] = {}
        self._by_isin: dict[str, dict] = {}
        self._by_symbol: dict[str, dict] = {}  # scrip_id → security
        self._search_corpus: list[str] = []
        self._loaded = False

    def _ensure_loaded(self, bse_instance=None):
        """Load securities from cache or BSE API."""
        if self._loaded:
            return

        age = get_securities_age()

        if age is not None and age < SECURITIES_TTL_HOURS:
            cached = load_securities()
            if cached:
                self._build_index(cached)
                logger.info(f"Loaded {len(cached)} securities from cache (age: {age:.1f}h)")
                return

        if bse_instance is None:
            cached = load_securities()
            if cached:
                self._build_index(cached)
                logger.info(f"Loaded {len(cached)} securities from stale cache (no BSE instance)")
                return
            raise RuntimeError("No cached securities and no BSE instance to fetch them")

        self._fetch_and_cache(bse_instance)

    def _fetch_and_cache(self, bse_instance):
        """Fetch all securities from BSE and cache them."""
        all_securities = []
        seen_codes = set()

        for group in GROUPS:
            try:
                secs = bse_instance.listSecurities(group=group)
                for sec in secs:
                    code = str(sec.get("SCRIP_CD", sec.get("scrip_code", sec.get("Scrip_Code", ""))))
                    if code and code not in seen_codes:
                        seen_codes.add(code)
                        all_securities.append(sec)
            except Exception as e:
                logger.warning(f"Failed to load group {group}: {e}")

        if all_securities:
            count = save_securities(all_securities)
            logger.info(f"Cached {count} securities from BSE API")
            # Reload from cache for normalized field names
            cached = load_securities()
            self._build_index(cached)
        else:
            # Fall back to stale cache
            cached = load_securities()
            if cached:
                self._build_index(cached)
            else:
                logger.error("No securities available from API or cache")

    def _build_index(self, securities: list[dict]):
        """Build in-memory search indices."""
        self._securities = securities
        self._by_code = {}
        self._by_isin = {}
        self._by_symbol = {}
        self._search_corpus = []

        for sec in securities:
            code = sec.get("scrip_code", "")
            isin = sec.get("isin", "")
            symbol = sec.get("scrip_id", "")

            if code:
                self._by_code[code] = sec
            if isin:
                self._by_isin[isin.upper()] = sec
            if symbol:
                self._by_symbol[symbol.upper()] = sec

            # Build search string: "SYMBOL | SCRIP_NAME | ISSUER_NAME"
            scrip_name = sec.get("scrip_name", "")
            issuer_name = sec.get("issuer_name", "")
            parts = [p for p in [symbol, scrip_name, issuer_name] if p]
            self._search_corpus.append(" | ".join(parts))

        self._loaded = True

    def resolve(self, query: str, bse_instance=None, top_n: int = 5, cutoff: int = 60) -> list[dict[str, Any]]:
        """
        Resolve a company query to BSE securities.

        Args:
            query: Company name, scrip code, or ISIN
            bse_instance: BSE instance for fetching securities if not cached
            top_n: Number of results to return
            cutoff: Minimum fuzzy match score (0-100)

        Returns:
            List of matches with score, sorted by score descending
        """
        self._ensure_loaded(bse_instance)

        query = query.strip()
        if not query:
            return []

        # Direct scrip code lookup
        if query.isdigit():
            sec = self._by_code.get(query)
            if sec:
                return [self._format_result(sec, 100)]
            return []

        # ISIN lookup
        if ISIN_PATTERN.match(query):
            sec = self._by_isin.get(query.upper())
            if sec:
                return [self._format_result(sec, 100)]
            return []

        # Exact symbol match (e.g., "TCS", "INFY", "RELIANCE")
        sec = self._by_symbol.get(query.upper())
        if sec:
            return [self._format_result(sec, 100)]

        # Prefix match on symbols (e.g., "rel" → "RELIANCE")
        query_upper = query.upper()
        prefix_matches = []
        for sym, sec in self._by_symbol.items():
            if sym.startswith(query_upper):
                prefix_matches.append(self._format_result(sec, 95))
        if prefix_matches:
            # Prioritize: group A (large-cap) first, then shorter symbols
            group_order = {"A": 0, "B": 1, "T": 2, "M": 3}
            prefix_matches.sort(key=lambda x: (
                group_order.get(x.get("group", ""), 9),
                len(x.get("scrip_id", "")),
                x["name"],
            ))
            return prefix_matches[:top_n]

        # Name-contains match (e.g., "tata motors" → "Tata Motors Ltd")
        query_lower = query.lower()
        name_matches = []
        for sec in self._securities:
            scrip_name = (sec.get("scrip_name", "") or "").lower()
            issuer_name = (sec.get("issuer_name", "") or "").lower()
            if query_lower in scrip_name or query_lower in issuer_name:
                name_matches.append(self._format_result(sec, 90))
        if name_matches:
            # Sort by: shortest name first (most exact match), then group A > B > etc.
            group_order = {"A": 0, "B": 1, "T": 2, "M": 3}
            name_matches.sort(key=lambda x: (
                len(x.get("name", "")),
                group_order.get(x.get("group", ""), 9),
            ))
            return name_matches[:top_n]

        # Fuzzy match against search corpus
        results = process.extract(
            query,
            self._search_corpus,
            scorer=fuzz.WRatio,
            limit=top_n,
            score_cutoff=cutoff,
        )

        matches = []
        for match_str, score, idx in results:
            sec = self._securities[idx]
            matches.append(self._format_result(sec, round(score, 1)))

        return matches

    def _format_result(self, sec: dict, score: float) -> dict[str, Any]:
        """Format a security record as a result dict."""
        return {
            "scrip_code": sec.get("scrip_code", ""),
            "scrip_id": sec.get("scrip_id", ""),
            "name": sec.get("scrip_name", ""),
            "issuer_name": sec.get("issuer_name", ""),
            "group": sec.get("scrip_group", ""),
            "isin": sec.get("isin", ""),
            "score": score,
        }

    def get_by_code(self, code: str, bse_instance=None) -> dict | None:
        """Get security by scrip code."""
        self._ensure_loaded(bse_instance)
        return self._by_code.get(code)
