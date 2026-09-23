"""Sandbox Monitor - FastAPI Proxy Server.

Provides a web interface for monitoring Interactive Sandbox Session states.
Proxies requests to the Sandbox API to handle CORS restrictions.
"""

from __future__ import annotations

import os
from pathlib import Path

from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import HTMLResponse, JSONResponse
from fastapi.staticfiles import StaticFiles

import httpx

# Sandbox service configuration
SANDBOX_HOST = os.getenv("SANDBOX_HOST", "http://127.0.0.1:8765")
SANDBOX_TIMEOUT = float(os.getenv("SANDBOX_TIMEOUT", "30.0"))

# Static files path
STATIC_DIR = Path(__file__).parent / "static"


def create_app() -> FastAPI:
    """Create and configure the FastAPI application."""
    app = FastAPI(
        title="ScienceEDA Sandbox Monitor",
        description="Monitor Interactive Sandbox Session States",
        version="1.0.0",
    )

    # CORS middleware - allow all origins for local development
    app.add_middleware(
        CORSMiddleware,
        allow_origins=["*"],
        allow_credentials=True,
        allow_methods=["GET"],
        allow_headers=["*"],
    )

    # Async HTTP client for proxying
    async_client: httpx.AsyncClient | None = None

    @app.middleware("http")
    async def lifespan_middleware(request: Request, call_next):
        nonlocal async_client
        if async_client is None:
            async_client = httpx.AsyncClient(
                timeout=httpx.Timeout(SANDBOX_TIMEOUT),
                follow_redirects=True,
            )
        response = await call_next(request)
        return response

    @app.get("/")
    async def root() -> HTMLResponse:
        """Serve the main HTML page."""
        index_path = STATIC_DIR / "index.html"
        if index_path.exists():
            return HTMLResponse(content=index_path.read_text(encoding="utf-8"))
        return HTMLResponse(
            content="<h1>index.html not found</h1><p>Please ensure static/index.html exists.</p>",
            status_code=404,
        )

    # API Proxy endpoints
    @app.get("/api/capabilities")
    async def get_capabilities() -> JSONResponse:
        """Proxy to Sandbox /v1/capabilities."""
        async with httpx.AsyncClient(timeout=httpx.Timeout(SANDBOX_TIMEOUT)) as client:
            resp = await client.get(f"{SANDBOX_HOST}/v1/capabilities")
            return JSONResponse(content=resp.json(), status_code=resp.status_code)

    @app.get("/api/sessions")
    async def list_sessions() -> JSONResponse:
        """Proxy to Sandbox /v1/workspace-sessions/list."""
        async with httpx.AsyncClient(timeout=httpx.Timeout(SANDBOX_TIMEOUT)) as client:
            resp = await client.get(f"{SANDBOX_HOST}/v1/workspace-sessions/list")
            return JSONResponse(content=resp.json(), status_code=resp.status_code)

    @app.get("/api/sessions/{session_id}")
    async def get_session(session_id: str) -> JSONResponse:
        """Proxy to Sandbox /v1/workspace-sessions/{id}."""
        async with httpx.AsyncClient(timeout=httpx.Timeout(SANDBOX_TIMEOUT)) as client:
            resp = await client.get(
                f"{SANDBOX_HOST}/v1/workspace-sessions/{session_id}"
            )
            return JSONResponse(content=resp.json(), status_code=resp.status_code)

    @app.get("/api/sessions/{session_id}/history")
    async def get_session_history(
        session_id: str,
        limit: int = 50,
        cursor: str | None = None,
        order: str = "asc",
    ) -> JSONResponse:
        """Proxy to Sandbox /v1/workspace-sessions/{id}/history."""
        # limit=0 means fetch all (no limit)
        params = {"limit": limit, "order": order} if limit > 0 else {"order": order}
        if cursor:
            params["cursor"] = cursor
        async with httpx.AsyncClient(timeout=httpx.Timeout(SANDBOX_TIMEOUT)) as client:
            resp = await client.get(
                f"{SANDBOX_HOST}/v1/workspace-sessions/{session_id}/history",
                params=params,
            )
            return JSONResponse(content=resp.json(), status_code=resp.status_code)

    @app.delete("/api/sessions/{session_id}")
    async def delete_session(session_id: str) -> JSONResponse:
        """Proxy to Sandbox DELETE /v1/workspace-sessions/{id}."""
        async with httpx.AsyncClient(timeout=httpx.Timeout(SANDBOX_TIMEOUT)) as client:
            resp = await client.delete(
                f"{SANDBOX_HOST}/v1/workspace-sessions/{session_id}"
            )
            return JSONResponse(content=resp.json(), status_code=resp.status_code)

    @app.get("/api/health")
    async def health_check() -> dict:
        """Health check endpoint."""
        try:
            async with httpx.AsyncClient(
                timeout=httpx.Timeout(5.0)
            ) as client:
                resp = await client.get(f"{SANDBOX_HOST}/is_alive")
                return {"status": "ok", "sandbox_reachable": True}
        except Exception as e:
            return {"status": "ok", "sandbox_reachable": False, "error": str(e)}

    # Mount static files
    if STATIC_DIR.exists():
        app.mount("/static", StaticFiles(directory=str(STATIC_DIR)), name="static")

    return app


app = create_app()


if __name__ == "__main__":
    import uvicorn

    port = int(os.getenv("MONITOR_PORT", "8766"))
    uvicorn.run(app, host="0.0.0.0", port=port)
