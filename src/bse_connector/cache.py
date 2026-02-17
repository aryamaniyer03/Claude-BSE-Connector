"""Document caching, smart chunking, and securities index for BSE filings."""

import hashlib
import json
import os
import re
import sqlite3
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any


# Cache location — respects BSE_CACHE_DIR env var for containerized deployments
# (Render/Railway/Docker use ephemeral filesystems, so we point to a writable path)
CACHE_DIR = Path(os.environ.get("BSE_CACHE_DIR", str(Path.home() / ".bse_mcp_cache")))
CACHE_DB = CACHE_DIR / "documents.db"


def get_cache_db() -> sqlite3.Connection:
    """Get or create the cache database."""
    CACHE_DIR.mkdir(parents=True, exist_ok=True)

    conn = sqlite3.connect(str(CACHE_DB))
    conn.row_factory = sqlite3.Row

    conn.executescript("""
        CREATE TABLE IF NOT EXISTS documents (
            id TEXT PRIMARY KEY,
            company_code TEXT,
            company_name TEXT,
            doc_type TEXT,
            headline TEXT,
            url TEXT,
            date TEXT,
            full_text TEXT,
            pages INTEGER,
            ocr_used INTEGER,
            cached_at TEXT,
            file_size INTEGER
        );

        CREATE INDEX IF NOT EXISTS idx_company ON documents(company_code);
        CREATE INDEX IF NOT EXISTS idx_doc_type ON documents(doc_type);
        CREATE INDEX IF NOT EXISTS idx_date ON documents(date);

        CREATE TABLE IF NOT EXISTS chunks (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            doc_id TEXT,
            chunk_type TEXT,
            chunk_index INTEGER,
            content TEXT,
            FOREIGN KEY (doc_id) REFERENCES documents(id)
        );

        CREATE INDEX IF NOT EXISTS idx_chunk_doc ON chunks(doc_id);
        CREATE INDEX IF NOT EXISTS idx_chunk_type ON chunks(chunk_type);

        CREATE TABLE IF NOT EXISTS securities (
            scrip_code TEXT PRIMARY KEY,
            scrip_id TEXT,
            scrip_name TEXT,
            issuer_name TEXT,
            scrip_group TEXT,
            isin TEXT,
            cached_at TEXT
        );

        CREATE INDEX IF NOT EXISTS idx_sec_name ON securities(scrip_name);
        CREATE INDEX IF NOT EXISTS idx_sec_issuer ON securities(issuer_name);
        CREATE INDEX IF NOT EXISTS idx_sec_isin ON securities(isin);
    """)

    conn.commit()
    return conn


# ---------------------------------------------------------------------------
# Securities cache (for fuzzy resolver)
# ---------------------------------------------------------------------------

SECURITIES_TTL_HOURS = 24


def get_securities_age() -> float | None:
    """Return age of securities cache in hours, or None if empty."""
    conn = get_cache_db()
    cursor = conn.execute("SELECT MIN(cached_at) as oldest FROM securities")
    row = cursor.fetchone()
    conn.close()

    if not row or not row["oldest"]:
        return None

    oldest = datetime.fromisoformat(row["oldest"])
    return (datetime.now() - oldest).total_seconds() / 3600


def save_securities(securities: list[dict]) -> int:
    """Bulk-save securities to cache. Returns count saved."""
    conn = get_cache_db()
    now = datetime.now().isoformat()

    conn.execute("DELETE FROM securities")

    for sec in securities:
        conn.execute(
            """INSERT OR REPLACE INTO securities
               (scrip_code, scrip_id, scrip_name, issuer_name, scrip_group, isin, cached_at)
               VALUES (?, ?, ?, ?, ?, ?, ?)""",
            (
                str(sec.get("scrip_code", sec.get("Scrip_Code", sec.get("SCRIP_CD", "")))),
                sec.get("scrip_id", ""),
                sec.get("scrip_name", sec.get("Scrip_Name", "")),
                sec.get("issuer_name", sec.get("Issuer_Name", sec.get("ISSUER_NAME", ""))),
                sec.get("scrip_group", sec.get("GROUP", sec.get("Scrip_Group", ""))),
                sec.get("isin", sec.get("ISIN_NUMBER", sec.get("ISIN", ""))),
                now,
            ),
        )

    conn.commit()
    count = conn.execute("SELECT COUNT(*) as c FROM securities").fetchone()["c"]
    conn.close()
    return count


def load_securities() -> list[dict]:
    """Load all securities from cache."""
    conn = get_cache_db()
    cursor = conn.execute("SELECT * FROM securities")
    results = [dict(row) for row in cursor.fetchall()]
    conn.close()
    return results


# ---------------------------------------------------------------------------
# Document cache
# ---------------------------------------------------------------------------

def doc_id_from_url(url: str) -> str:
    """Generate a unique document ID from URL."""
    return hashlib.md5(url.encode()).hexdigest()


def is_cached(url: str) -> bool:
    """Check if document is already cached."""
    conn = get_cache_db()
    cursor = conn.execute("SELECT 1 FROM documents WHERE url = ?", (url,))
    result = cursor.fetchone() is not None
    conn.close()
    return result


def get_cached_document(url: str) -> dict | None:
    """Get cached document by URL."""
    conn = get_cache_db()
    cursor = conn.execute("SELECT * FROM documents WHERE url = ?", (url,))
    row = cursor.fetchone()
    conn.close()

    if row:
        return dict(row)
    return None


def get_company_documents(company_code: str, doc_types: list[str] | None = None) -> list[dict]:
    """Get all cached documents for a company."""
    conn = get_cache_db()

    if doc_types:
        placeholders = ",".join("?" * len(doc_types))
        cursor = conn.execute(
            f"SELECT * FROM documents WHERE company_code = ? AND doc_type IN ({placeholders}) ORDER BY date DESC",
            [company_code] + doc_types
        )
    else:
        cursor = conn.execute(
            "SELECT * FROM documents WHERE company_code = ? ORDER BY date DESC",
            (company_code,)
        )

    results = [dict(row) for row in cursor.fetchall()]
    conn.close()
    return results


def cache_document(
    url: str,
    company_code: str,
    company_name: str,
    doc_type: str,
    headline: str,
    date: str,
    full_text: str,
    pages: int,
    ocr_used: bool,
) -> str:
    """Cache a document and create chunks."""
    doc_id = doc_id_from_url(url)

    conn = get_cache_db()

    conn.execute("""
        INSERT OR REPLACE INTO documents
        (id, company_code, company_name, doc_type, headline, url, date, full_text, pages, ocr_used, cached_at, file_size)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, (
        doc_id, company_code, company_name, doc_type, headline, url, date,
        full_text, pages, 1 if ocr_used else 0, datetime.now().isoformat(),
        len(full_text)
    ))

    conn.execute("DELETE FROM chunks WHERE doc_id = ?", (doc_id,))

    chunks = create_smart_chunks(full_text, doc_type)

    for i, chunk in enumerate(chunks):
        conn.execute("""
            INSERT INTO chunks (doc_id, chunk_type, chunk_index, content)
            VALUES (?, ?, ?, ?)
        """, (doc_id, chunk["type"], i, chunk["content"]))

    conn.commit()
    conn.close()

    return doc_id


def create_smart_chunks(text: str, doc_type: str) -> list[dict]:
    """Create smart chunks from document text based on document type."""
    chunks = []

    pages = re.split(r'--- Page \d+[^-]*---', text)

    patterns = {
        "guidance": [
            r"(?i)(outlook|guidance|future|forward.looking|growth.plan|expansion|target|goal|expect|anticipate)",
            r"(?i)(fy\d{2,4}|next.year|coming.quarter|pipeline)",
        ],
        "financials": [
            r"(?i)(revenue|ebitda|profit|margin|eps|earning|crore|billion|million|\d+%)",
            r"(?i)(yoy|qoq|growth|decline|increase|decrease)",
        ],
        "qa": [
            r"(?i)(question|answer|q:|a:|analyst|participant)",
            r"(?i)(could you|can you|what is|how do|why did)",
        ],
        "segment": [
            r"(?i)(segment|business.unit|division|vertical)",
            r"(?i)(retail|digital|jio|o2c|energy|telecom)",
        ],
        "summary": [
            r"(?i)(highlight|key.point|summary|overview|at.a.glance)",
        ],
    }

    current_chunk: list[str] = []
    current_type = "general"
    chunk_size = 0
    max_chunk_size = 4000

    for page in pages:
        if not page.strip():
            continue

        page_lower = page.lower()
        detected_type = "general"

        for chunk_type, type_patterns in patterns.items():
            matches = sum(1 for p in type_patterns if re.search(p, page_lower))
            if matches >= 1:
                detected_type = chunk_type
                break

        if detected_type != current_type or chunk_size + len(page) > max_chunk_size:
            if current_chunk:
                chunks.append({
                    "type": current_type,
                    "content": "\n".join(current_chunk)
                })
            current_chunk = [page]
            current_type = detected_type
            chunk_size = len(page)
        else:
            current_chunk.append(page)
            chunk_size += len(page)

    if current_chunk:
        chunks.append({
            "type": current_type,
            "content": "\n".join(current_chunk)
        })

    return chunks


def get_relevant_chunks(
    company_code: str,
    query: str,
    chunk_types: list[str] | None = None,
    max_chunks: int = 10,
    max_chars: int = 50000,
) -> list[dict]:
    """Get relevant chunks for a query from cached documents."""
    conn = get_cache_db()

    query_lower = query.lower()

    if chunk_types is None:
        chunk_types = []

        if any(w in query_lower for w in ["future", "plan", "outlook", "guidance", "expect", "target", "growth"]):
            chunk_types.extend(["guidance", "summary"])

        if any(w in query_lower for w in ["revenue", "profit", "margin", "financial", "result", "earning"]):
            chunk_types.extend(["financials", "summary"])

        if any(w in query_lower for w in ["management", "said", "comment", "question", "answer"]):
            chunk_types.extend(["qa", "guidance"])

        if any(w in query_lower for w in ["segment", "business", "retail", "jio", "digital"]):
            chunk_types.extend(["segment"])

        if not chunk_types:
            chunk_types = ["guidance", "summary", "financials"]

    chunk_types = list(dict.fromkeys(chunk_types))

    docs = get_company_documents(company_code)

    if not docs:
        conn.close()
        return []

    results = []
    total_chars = 0

    for chunk_type in chunk_types:
        if len(results) >= max_chunks or total_chars >= max_chars:
            break

        cursor = conn.execute("""
            SELECT c.*, d.headline, d.date, d.doc_type as document_type, d.company_name
            FROM chunks c
            JOIN documents d ON c.doc_id = d.id
            WHERE d.company_code = ? AND c.chunk_type = ?
            ORDER BY d.date DESC
        """, (company_code, chunk_type))

        for row in cursor:
            if len(results) >= max_chunks or total_chars >= max_chars:
                break

            chunk = dict(row)
            content_len = len(chunk["content"])

            if total_chars + content_len <= max_chars:
                results.append({
                    "chunk_type": chunk["chunk_type"],
                    "document_type": chunk["document_type"],
                    "document_date": chunk["date"],
                    "headline": chunk["headline"],
                    "company": chunk["company_name"],
                    "content": chunk["content"],
                })
                total_chars += content_len

    conn.close()
    return results


def get_cache_stats() -> dict:
    """Get cache statistics."""
    conn = get_cache_db()

    stats = {}

    cursor = conn.execute("SELECT COUNT(*) as count FROM documents")
    stats["total_documents"] = cursor.fetchone()["count"]

    cursor = conn.execute("SELECT COUNT(*) as count FROM chunks")
    stats["total_chunks"] = cursor.fetchone()["count"]

    cursor = conn.execute("""
        SELECT company_name, COUNT(*) as count
        FROM documents
        GROUP BY company_code
        ORDER BY count DESC
        LIMIT 10
    """)
    stats["by_company"] = [dict(row) for row in cursor.fetchall()]

    cursor = conn.execute("SELECT SUM(file_size) as total FROM documents")
    stats["total_size_bytes"] = cursor.fetchone()["total"] or 0

    cursor = conn.execute("SELECT COUNT(*) as count FROM securities")
    stats["securities_cached"] = cursor.fetchone()["count"]

    stats["cache_location"] = str(CACHE_DB)

    conn.close()
    return stats


def clear_company_cache(company_code: str) -> int:
    """Clear cache for a specific company. Returns number of documents deleted."""
    conn = get_cache_db()

    cursor = conn.execute("SELECT id FROM documents WHERE company_code = ?", (company_code,))
    doc_ids = [row["id"] for row in cursor.fetchall()]

    for doc_id in doc_ids:
        conn.execute("DELETE FROM chunks WHERE doc_id = ?", (doc_id,))

    conn.execute("DELETE FROM documents WHERE company_code = ?", (company_code,))

    conn.commit()
    conn.close()

    return len(doc_ids)


def clear_all_cache() -> int:
    """Clear entire cache. Returns number of documents deleted."""
    conn = get_cache_db()

    cursor = conn.execute("SELECT COUNT(*) as count FROM documents")
    count = cursor.fetchone()["count"]

    conn.execute("DELETE FROM chunks")
    conn.execute("DELETE FROM documents")

    conn.commit()
    conn.close()

    return count
