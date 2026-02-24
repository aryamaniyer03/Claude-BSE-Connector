"""
Remote MCP server for Claude.ai connectors — Streamable HTTP transport.

Deploy this to any cloud provider (Railway, Fly.io, Render, etc.) and add
the URL as a custom connector in Claude.ai Settings → Connectors.

Usage:
    bse-connector-http                    # starts on port 8000
    PORT=3000 bse-connector-http          # starts on port 3000

The connector URL for Claude.ai is: https://eqplus.ai/mcp
"""

import contextlib
import logging
import os
import sys
from collections.abc import AsyncIterator
from pathlib import Path

import uvicorn
from starlette.applications import Starlette
from starlette.middleware import Middleware
from starlette.middleware.cors import CORSMiddleware
from starlette.requests import Request
from starlette.responses import JSONResponse, FileResponse, Response
from starlette.routing import Route

from mcp.server.streamable_http_manager import StreamableHTTPSessionManager

from .server import server, _get_client

# Log to stderr (stdout is reserved for MCP stdio protocol in local mode)
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(name)s] %(levelname)s: %(message)s",
    stream=sys.stderr,
)
logger = logging.getLogger("bse-connector-http")

# Static assets directory
STATIC_DIR = Path(__file__).parent / "static"

# Base URL — uses custom domain if configured, otherwise falls back to Render URL
BASE_URL = os.environ.get("BASE_URL", "https://claude-bse-connector.onrender.com")

# Streamable HTTP session manager — stateless mode for simplicity on free/starter tiers
session_manager = StreamableHTTPSessionManager(
    app=server,
    stateless=True,
    json_response=False,
)


class MCPMiddleware:
    """ASGI middleware that intercepts /mcp requests and delegates to the
    StreamableHTTPSessionManager. This avoids the Route vs Mount issue —
    Route can't pass raw ASGI, and Mount redirects /mcp to /mcp/.
    """

    def __init__(self, app):
        self.app = app

    async def __call__(self, scope, receive, send):
        if scope["type"] == "http" and scope["path"].rstrip("/") == "/mcp":
            await session_manager.handle_request(scope, receive, send)
        else:
            await self.app(scope, receive, send)


async def health(request: Request) -> JSONResponse:
    """Health check endpoint for Render / load balancers."""
    return JSONResponse({"status": "ok", "server": "eqplus-bse-connector", "version": "0.1.0"})


async def logo(request: Request) -> FileResponse:
    """Serve the EQ+ logo."""
    return FileResponse(STATIC_DIR / "logo.svg", media_type="image/svg+xml")


async def well_known_mcp(request: Request) -> JSONResponse:
    """MCP server discovery — .well-known/mcp.json

    Allows Claude.ai and other MCP clients to discover server metadata,
    including the logo/icon, description, and endpoint URL.
    """
    return JSONResponse({
        "name": "EQ+ BSE Connector",
        "description": (
            "BSE India Corporate Filings & Financials API — company search, "
            "quarterly financials (Revenue, EBITDA, PAT, EPS), corporate filings, "
            "announcements, analyst consensus, concall transcripts, and more."
        ),
        "icon": f"{BASE_URL}/logo.svg",
        "url": f"{BASE_URL}/mcp",
        "transport": "streamable-http",
        "version": "0.1.0",
        "provider": {
            "name": "EQ+",
            "url": "https://eqplus.ai",
        },
    })


@contextlib.asynccontextmanager
async def lifespan(app: Starlette) -> AsyncIterator[None]:
    """Manage startup/shutdown lifecycle."""
    logger.info("Pre-warming BSE client and securities index...")
    try:
        client = _get_client()
        client.search_company("test", top_n=1)
        logger.info("BSE client and securities index ready")
    except Exception as e:
        logger.warning(f"Pre-warm failed (will retry on first request): {e}")

    async with session_manager.run():
        yield


# Starlette app — health, logo, discovery; MCP is handled by middleware
_inner_app = Starlette(
    debug=False,
    routes=[
        Route("/health", health, methods=["GET"]),
        Route("/logo.svg", logo, methods=["GET"]),
        Route("/.well-known/mcp.json", well_known_mcp, methods=["GET"]),
    ],
    middleware=[
        Middleware(
            CORSMiddleware,
            allow_origins=["*"],
            allow_methods=["GET", "POST", "DELETE", "OPTIONS"],
            allow_headers=["*"],
            expose_headers=["Mcp-Session-Id"],
        ),
    ],
    lifespan=lifespan,
)

# Wrap with MCP middleware so /mcp and /mcp/ both work
app = MCPMiddleware(_inner_app)


def main():
    """Entry point for the remote HTTP server."""
    port = int(os.environ.get("PORT", 8000))
    host = os.environ.get("HOST", "0.0.0.0")

    logger.info(f"Starting EQ+ BSE Connector on {host}:{port}")
    logger.info(f"MCP endpoint: {BASE_URL}/mcp")
    logger.info(f"Discovery: {BASE_URL}/.well-known/mcp.json")

    uvicorn.run(
        app,
        host=host,
        port=port,
        log_level="info",
        # Production settings
        timeout_keep_alive=65,      # slightly above Render's 60s LB timeout
        timeout_graceful_shutdown=10,  # clean shutdown on SIGTERM
    )


if __name__ == "__main__":
    main()
