"""MCP Server for BSE India — fuzzy search, quarterly financials, corporate filings."""

import json
import logging
import sys
import traceback
from datetime import date

from mcp.server import Server
from mcp.server.stdio import stdio_server
from mcp.types import TextContent, Tool

from .bse_client import BSEClient, fetch_pdf_text
from .categories import (
    CATEGORY_DESCRIPTIONS,
    PURPOSE_DESCRIPTIONS,
    Category,
    Purpose,
)

# Log to stderr so it doesn't corrupt the stdio MCP protocol
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(name)s] %(levelname)s: %(message)s",
    stream=sys.stderr,
)
logger = logging.getLogger("bse-connector")

# Long-lived client — avoids re-creating BSE browser session per tool call
_client: BSEClient | None = None


def _get_client() -> BSEClient:
    """Get or create the singleton BSEClient."""
    global _client
    if _client is None:
        _client = BSEClient()
        _client.__enter__()
        logger.info("BSEClient initialized")
    return _client


# Maximum chars to return in a single tool response (protect Claude's context)
MAX_RESPONSE_CHARS = 80_000


def _truncate(text: str, limit: int = MAX_RESPONSE_CHARS) -> str:
    """Truncate response text if it exceeds the limit."""
    if len(text) <= limit:
        return text
    return text[:limit] + f"\n\n... [TRUNCATED — {len(text) - limit} chars omitted]"


def _json_response(data: dict, **kwargs) -> list[TextContent]:
    """Serialize to JSON, truncate if needed, return as TextContent."""
    text = json.dumps(data, indent=2, default=str, **kwargs)
    return [TextContent(type="text", text=_truncate(text))]


def _error_response(message: str) -> list[TextContent]:
    """Return a structured error response."""
    return [TextContent(type="text", text=json.dumps({"error": message}))]


# Initialize MCP server
server = Server(
    "bse-connector",
    instructions=(
        "BSE India Corporate Filings & Financials API — Use this for ANY question about Indian companies/stocks. "
        "Covers: company search, quarterly financials (Revenue, EBITDA, PAT, EPS), "
        "corporate filings, announcements, dividends, stock splits, concall transcripts, "
        "investor presentations, management guidance, and any BSE announcements.\n\n"
        "TOOL SELECTION GUIDE:\n"
        "- Company lookup (fuzzy): 'search_company' — handles partial names like 'rel' → Reliance\n"
        "- Quarterly P&L numbers: 'get_quarterly_financials' — structured Revenue/EBITDA/PAT/EPS\n"
        "- Analyst consensus: 'get_analyst_consensus' — target prices, Buy/Sell ratings, EPS/revenue forecasts\n"
        "- Broad research (plans, outlook): 'research_company' — auto-fetches & caches transcripts\n"
        "- Specific filings: 'get_announcements' then 'fetch_document'\n"
        "- Dividends/splits/bonuses: 'get_corporate_actions'\n"
        "- Upcoming results dates: 'get_result_calendar'\n"
        "- Stock price/quote: 'get_company_info'\n\n"
        "All tools accept company names, BSE/NSE symbols, or BSE scrip codes. "
        "You do NOT need to call search_company first — all tools resolve company names internally."
    ),
)


@server.list_tools()
async def list_tools() -> list[Tool]:
    """List available BSE tools."""
    return [
        Tool(
            name="search_company",
            description=(
                "Fuzzy search for a company on BSE India. Handles partial names, "
                "symbols, scrip codes, and ISINs. Returns top 5 matches with confidence scores. "
                "Examples: 'rel' → Reliance Industries, 'TCS', '500325', 'INE002A01018'. "
                "Other tools resolve names internally — you only need this for disambiguation."
            ),
            inputSchema={
                "type": "object",
                "properties": {
                    "query": {
                        "type": "string",
                        "description": "Company name (partial OK), stock symbol, BSE scrip code, or ISIN",
                    }
                },
                "required": ["query"],
            },
        ),
        Tool(
            name="get_quarterly_financials",
            description=(
                "Get structured quarterly P&L data for an Indian company. "
                "Returns up to 6 quarters of: Revenue, Expenses, Operating Profit, OPM%, "
                "Other Income, Interest, Depreciation, PBT, Tax%, Net Profit, EPS, "
                "EBITDA, and EBITDA Margin. All values in Crores (INR). "
                "Source: Yahoo Finance. Accepts company name, symbol, or scrip code."
            ),
            inputSchema={
                "type": "object",
                "properties": {
                    "company": {
                        "type": "string",
                        "description": "Company name, symbol, or scrip code (e.g., 'TCS', 'Reliance', '500325')",
                    }
                },
                "required": ["company"],
            },
        ),
        Tool(
            name="get_analyst_consensus",
            description=(
                "Get analyst consensus estimates for an Indian company from Yahoo Finance. "
                "Returns: target price (mean/median/high/low), analyst recommendations "
                "(Strong Buy/Buy/Hold/Sell/Strong Sell counts), EPS estimates (current & next year), "
                "revenue estimates, EPS trend (7d/30d/60d/90d revisions), EPS revision counts, "
                "growth estimates, and key valuation ratios (forward PE, P/B, EV/EBITDA). "
                "Source: Yahoo Finance. Accepts company name, symbol, or scrip code."
            ),
            inputSchema={
                "type": "object",
                "properties": {
                    "company": {
                        "type": "string",
                        "description": "Company name, symbol, or scrip code (e.g., 'TCS', 'Reliance', 'Apar Industries')",
                    }
                },
                "required": ["company"],
            },
        ),
        Tool(
            name="get_announcements",
            description=(
                "Fetch corporate announcements/filings from BSE India. "
                "Filter by company, category, subcategory, keyword, and date range. "
                "Supports quarter notation like 'Q3 2025' or 'Q1 FY25'. "
                "Use keyword='transcript' for concall transcripts, 'presentation' for investor presentations, "
                "'results' for financial results. Returns attachment URLs that can be read with fetch_document."
            ),
            inputSchema={
                "type": "object",
                "properties": {
                    "company": {
                        "type": "string",
                        "description": "Company name, symbol, or scrip code. Omit to search all companies.",
                    },
                    "category": {
                        "type": "string",
                        "description": "Category: Result, Board Meeting, AGM/EGM, Corp. Action, Company Update, Insider Trading, New Listing, Others",
                    },
                    "subcategory": {
                        "type": "string",
                        "description": "Precise subcategory: 'Investor Presentation', 'Earnings Call Transcript', 'Financial Results', 'Press Release / Media Release'",
                    },
                    "keyword": {
                        "type": "string",
                        "description": "Headline filter: 'transcript', 'presentation', 'results', 'annual report', 'press release', 'acquisition', 'credit rating'",
                    },
                    "date_range": {
                        "type": "string",
                        "description": "Date range: 'Q3 2025', 'Q1 FY25', '2025', 'last 30 days', 'past 6 months'",
                    },
                    "page": {
                        "type": "integer",
                        "description": "Page number (default: 1)",
                        "default": 1,
                    },
                },
            },
        ),
        Tool(
            name="get_corporate_actions",
            description=(
                "Fetch corporate actions: dividends, bonuses, stock splits, rights issues, buybacks. "
                "Filter by company, action type, and date range."
            ),
            inputSchema={
                "type": "object",
                "properties": {
                    "company": {
                        "type": "string",
                        "description": "Company name, symbol, or scrip code (optional)",
                    },
                    "action_type": {
                        "type": "string",
                        "description": "Type of action",
                        "enum": ["dividend", "bonus", "split", "rights", "buyback", "all"],
                    },
                    "date_range": {
                        "type": "string",
                        "description": "Date range: 'Q3 2025', '2025', 'last 90 days'",
                    },
                },
            },
        ),
        Tool(
            name="get_result_calendar",
            description=(
                "Fetch earnings announcement calendar — when companies will announce results. "
                "Shows upcoming and past result dates."
            ),
            inputSchema={
                "type": "object",
                "properties": {
                    "company": {
                        "type": "string",
                        "description": "Company name, symbol, or scrip code. Omit for all companies.",
                    },
                    "date_range": {
                        "type": "string",
                        "description": "Date range: 'next 30 days', 'Q4 2025', 'last 7 days'",
                    },
                },
            },
        ),
        Tool(
            name="get_company_info",
            description=(
                "Get company details + current stock price, 52-week high/low from BSE."
            ),
            inputSchema={
                "type": "object",
                "properties": {
                    "company": {
                        "type": "string",
                        "description": "Company name, symbol, or scrip code",
                    }
                },
                "required": ["company"],
            },
        ),
        Tool(
            name="fetch_document",
            description=(
                "Extract text from a BSE PDF filing (transcript, presentation, result). "
                "Use the attachment_url from get_announcements results."
            ),
            inputSchema={
                "type": "object",
                "properties": {
                    "url": {
                        "type": "string",
                        "description": "The attachment_url from an announcement result",
                    },
                    "max_pages": {
                        "type": "integer",
                        "description": "Maximum pages to extract (default: 50)",
                        "default": 50,
                    },
                },
                "required": ["url"],
            },
        ),
        Tool(
            name="research_company",
            description=(
                "Deep company research — auto-downloads transcripts, presentations, and results, "
                "caches them locally, and returns relevant chunks based on your query. "
                "First call fetches documents (slow). Subsequent calls are instant from cache. "
                "Best for: future plans, management guidance, growth outlook, strategy."
            ),
            inputSchema={
                "type": "object",
                "properties": {
                    "company": {
                        "type": "string",
                        "description": "Company name or symbol",
                    },
                    "query": {
                        "type": "string",
                        "description": "Your question — used to select relevant document chunks",
                    },
                    "focus": {
                        "type": "string",
                        "description": "Focus area: 'all', 'guidance', 'financials', 'transcripts', 'annual'",
                        "default": "all",
                    },
                    "periods": {
                        "type": "integer",
                        "description": "Number of quarters to fetch (default: 3, max: 5)",
                        "default": 3,
                    },
                },
                "required": ["company"],
            },
        ),
        Tool(
            name="get_categories",
            description="List available announcement categories and corporate action types for filtering.",
            inputSchema={
                "type": "object",
                "properties": {},
            },
        ),
        Tool(
            name="get_cache_stats",
            description="Show local document cache statistics — documents cached, companies, total size.",
            inputSchema={
                "type": "object",
                "properties": {},
            },
        ),
    ]


@server.call_tool()
async def call_tool(name: str, arguments: dict) -> list[TextContent]:
    """Handle tool calls with error handling."""
    try:
        client = _get_client()
        return _dispatch(name, arguments, client)
    except Exception as e:
        logger.error(f"Tool {name} failed: {e}\n{traceback.format_exc()}")
        return _error_response(f"Tool '{name}' failed: {str(e)}")


def _dispatch(name: str, arguments: dict, client: BSEClient) -> list[TextContent]:
    """Dispatch tool call to the appropriate handler."""

    if name == "search_company":
        results = client.search_company(arguments["query"])
        if results:
            return _json_response(results)
        return _error_response(f"No matches found for: {arguments['query']}")

    elif name == "get_quarterly_financials":
        result = client.get_quarterly_financials(arguments["company"])
        return _json_response(result)

    elif name == "get_analyst_consensus":
        result = client.get_analyst_consensus(arguments["company"])
        return _json_response(result)

    elif name == "get_announcements":
        result = client.get_announcements(
            company=arguments.get("company"),
            category=arguments.get("category"),
            subcategory=arguments.get("subcategory"),
            keyword=arguments.get("keyword"),
            date_range=arguments.get("date_range"),
            page=arguments.get("page", 1),
        )
        return _json_response(result)

    elif name == "get_corporate_actions":
        result = client.get_corporate_actions(
            company=arguments.get("company"),
            action_type=arguments.get("action_type"),
            date_range=arguments.get("date_range"),
        )
        return _json_response(result)

    elif name == "get_result_calendar":
        result = client.get_result_calendar(
            company=arguments.get("company"),
            date_range=arguments.get("date_range"),
        )
        return _json_response(result)

    elif name == "get_company_info":
        result = client.get_company_info(arguments["company"])
        return _json_response(result)

    elif name == "get_categories":
        categories = {
            "announcement_categories": {
                cat.name: {"value": cat.value, "description": desc}
                for cat, desc in CATEGORY_DESCRIPTIONS.items()
            },
            "corporate_action_types": {
                p.name: {"code": p.value, "description": desc}
                for p, desc in PURPOSE_DESCRIPTIONS.items()
            },
            "quarter_format_help": {
                "calendar_year": "Q1=Jan-Mar, Q2=Apr-Jun, Q3=Jul-Sep, Q4=Oct-Dec",
                "indian_fy": "Q1 FY25=Apr-Jun 2024, Q2 FY25=Jul-Sep 2024, Q3 FY25=Oct-Dec 2024, Q4 FY25=Jan-Mar 2025",
                "examples": ["Q3 2025", "Q1 FY25", "2025", "last 30 days"],
            },
        }
        return _json_response(categories)

    elif name == "fetch_document":
        url = arguments.get("url", "")
        max_pages = arguments.get("max_pages", 50)
        result = fetch_pdf_text(url, max_pages=max_pages)
        return _json_response(result)

    elif name == "research_company":
        result = client.research_company(
            company=arguments["company"],
            query=arguments.get("query", ""),
            focus=arguments.get("focus", "all"),
            periods=arguments.get("periods", 3),
        )
        return _json_response(result)

    elif name == "get_cache_stats":
        from .cache import get_cache_stats
        result = get_cache_stats()
        return _json_response(result)

    else:
        return _error_response(f"Unknown tool: {name}")


async def run_server():
    """Run the MCP server."""
    logger.info("Starting BSE Connector MCP server")
    async with stdio_server() as (read_stream, write_stream):
        await server.run(read_stream, write_stream, server.create_initialization_options())


def main():
    """Entry point."""
    import asyncio
    asyncio.run(run_server())


if __name__ == "__main__":
    main()
