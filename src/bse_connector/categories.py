"""BSE announcement categories and purpose codes."""

from enum import Enum


class Category(str, Enum):
    """BSE announcement categories."""

    AGM_EGM = "AGM/EGM"
    BOARD_MEETING = "Board Meeting"
    COMPANY_UPDATE = "Company Update"
    CORP_ACTION = "Corp. Action"
    INSIDER_TRADING = "Insider Trading / SAST"
    NEW_LISTING = "New Listing"
    RESULT = "Result"
    INTEGRATED_FILING = "Integrated Filing"
    OTHERS = "Others"
    ALL = "-1"  # No filter


class Purpose(str, Enum):
    """Corporate action purpose codes."""

    BONUS = "P5"
    BUYBACK = "P6"
    DIVIDEND = "P9"
    PREFERENCE_DIVIDEND = "P10"
    SPLIT = "P26"
    DELISTING = "P29"
    RIGHTS = "P18"
    AGM = "P1"
    EGM = "P2"
    ALL = None


class Segment(str, Enum):
    """Market segments."""

    EQUITY = "equity"
    DEBT = "debt"
    MF = "mf"


# Human-readable category descriptions for AI
CATEGORY_DESCRIPTIONS = {
    Category.AGM_EGM: "Annual/Extraordinary General Meetings - shareholder meetings, voting results",
    Category.BOARD_MEETING: "Board meeting outcomes, approvals, resolutions",
    Category.COMPANY_UPDATE: "General company updates and disclosures",
    Category.CORP_ACTION: "Corporate actions - dividends, splits, bonuses, rights",
    Category.INSIDER_TRADING: "Insider trading disclosures and SAST regulations",
    Category.NEW_LISTING: "New listings and IPO related announcements",
    Category.RESULT: "Financial results, earnings, concall transcripts, investor presentations",
    Category.INTEGRATED_FILING: "Integrated regulatory filings",
    Category.OTHERS: "Other announcements",
}

# Purpose code descriptions
PURPOSE_DESCRIPTIONS = {
    Purpose.BONUS: "Bonus shares issued to existing shareholders",
    Purpose.BUYBACK: "Company buying back its own shares",
    Purpose.DIVIDEND: "Cash dividend payments",
    Purpose.PREFERENCE_DIVIDEND: "Dividend on preference shares",
    Purpose.SPLIT: "Stock split - share subdivision",
    Purpose.DELISTING: "Delisting from exchange",
    Purpose.RIGHTS: "Rights issue - new shares to existing holders",
    Purpose.AGM: "Annual General Meeting",
    Purpose.EGM: "Extraordinary General Meeting",
}


def get_category_by_name(name: str) -> Category | None:
    """Get category enum by name (case-insensitive partial match)."""
    name_lower = name.lower()

    mappings = {
        "agm": Category.AGM_EGM,
        "egm": Category.AGM_EGM,
        "meeting": Category.BOARD_MEETING,
        "board": Category.BOARD_MEETING,
        "update": Category.COMPANY_UPDATE,
        "action": Category.CORP_ACTION,
        "corporate": Category.CORP_ACTION,
        "dividend": Category.CORP_ACTION,
        "bonus": Category.CORP_ACTION,
        "split": Category.CORP_ACTION,
        "insider": Category.INSIDER_TRADING,
        "sast": Category.INSIDER_TRADING,
        "listing": Category.NEW_LISTING,
        "ipo": Category.NEW_LISTING,
        "result": Category.RESULT,
        "earnings": Category.RESULT,
        "concall": Category.RESULT,
        "transcript": Category.RESULT,
        "quarterly": Category.RESULT,
        "annual": Category.RESULT,
        "financial": Category.RESULT,
        "integrated": Category.INTEGRATED_FILING,
        "filing": Category.INTEGRATED_FILING,
    }

    for key, cat in mappings.items():
        if key in name_lower:
            return cat

    for cat in Category:
        if cat.value.lower() == name_lower:
            return cat

    return None


def get_purpose_by_name(name: str) -> Purpose | None:
    """Get purpose enum by name (case-insensitive partial match)."""
    name_lower = name.lower()

    mappings = {
        "bonus": Purpose.BONUS,
        "buyback": Purpose.BUYBACK,
        "dividend": Purpose.DIVIDEND,
        "split": Purpose.SPLIT,
        "delist": Purpose.DELISTING,
        "rights": Purpose.RIGHTS,
        "agm": Purpose.AGM,
        "egm": Purpose.EGM,
    }

    for key, purpose in mappings.items():
        if key in name_lower:
            return purpose

    return None
