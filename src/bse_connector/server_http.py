"""
Remote MCP server for Claude.ai connectors — SSE transport over HTTP.

Deploy this to any cloud provider (Railway, Fly.io, Render, etc.) and add
the URL as a custom connector in Claude.ai Settings → Connectors.

Usage:
    bse-connector-http                    # starts on port 8000
    PORT=3000 bse-connector-http          # starts on port 3000

The connector URL for Claude.ai is: https://your-domain.com/sse
"""

import logging
import os
import sys

import uvicorn
from starlette.applications import Starlette
from starlette.middleware import Middleware
from starlette.middleware.cors import CORSMiddleware
from starlette.requests import Request
from starlette.responses import JSONResponse, Response
from starlette.routing import Mount, Route

from mcp.server.sse import SseServerTransport

from .server import server, _get_client

# Log to stderr (stdout is reserved for MCP stdio protocol in local mode)
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(name)s] %(levelname)s: %(message)s",
    stream=sys.stderr,
)
logger = logging.getLogger("bse-connector-http")

# SSE transport — the /messages endpoint is where clients POST tool calls
sse = SseServerTransport("/messages")


async def handle_sse(request: Request) -> Response:
    """SSE endpoint — Claude.ai connects here."""
    client_ip = request.client.host if request.client else "unknown"
    logger.info(f"New SSE connection from {client_ip}")
    async with sse.connect_sse(request.scope, request.receive, request._send) as streams:
        await server.run(
            streams[0],
            streams[1],
            server.create_initialization_options(),
        )
    return Response()


async def health(request: Request) -> JSONResponse:
    """Health check endpoint for Render / load balancers."""
    return JSONResponse({"status": "ok", "server": "bse-connector", "version": "0.1.0"})


async def on_startup():
    """Pre-warm the BSE client and securities index on startup."""
    logger.info("Pre-warming BSE client and securities index...")
    try:
        client = _get_client()
        # Trigger securities index load (fetches ~4800 securities from BSE API)
        client.search_company("test", top_n=1)
        logger.info("BSE client and securities index ready")
    except Exception as e:
        # Non-fatal — will retry on first request
        logger.warning(f"Pre-warm failed (will retry on first request): {e}")


# Starlette app
app = Starlette(
    debug=False,
    routes=[
        Route("/health", health, methods=["GET"]),
        Route("/sse", handle_sse, methods=["GET"]),
        Mount("/messages", app=sse.handle_post_message),
    ],
    middleware=[
        Middleware(
            CORSMiddleware,
            allow_origins=["*"],
            allow_methods=["GET", "POST", "OPTIONS"],
            allow_headers=["*"],
            expose_headers=["Mcp-Session-Id"],
        ),
    ],
    on_startup=[on_startup],
)


def main():
    """Entry point for the remote HTTP server."""
    port = int(os.environ.get("PORT", 8000))
    host = os.environ.get("HOST", "0.0.0.0")

    logger.info(f"Starting BSE Connector HTTP server on {host}:{port}")
    logger.info(f"Claude.ai connector URL: http://{host}:{port}/sse")

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
