"""Health check routes for the Exchange API.

This module provides health check endpoints:
- /health: Basic health check
- /healthz: Health check with version info
- /readyz: Readiness probe with Discogs connectivity check
"""

from __future__ import annotations

import os
import subprocess
from typing import Any, Dict

from fastapi import APIRouter
from fastapi.concurrency import run_in_threadpool
from fastapi.responses import JSONResponse

from exchange.clients.discogs_client import discogs_identity

from exchange.app.handlers.utils import with_cid

router = APIRouter(tags=["health"])

# Application version and build info
APP_VERSION = "m3.0.0"


def _detect_git_sha() -> str:
    """Detect the git SHA for the current build.

    Returns:
        Git SHA string or 'unknown'
    """
    env_sha = os.getenv("APP_BUILD_SHA")
    if env_sha:
        return env_sha
    try:
        return (
            subprocess.check_output(
                ["git", "rev-parse", "--short", "HEAD"], stderr=subprocess.DEVNULL
            )
            .decode("utf-8")
            .strip()
        )
    except Exception:
        return "unknown"


APP_BUILD = _detect_git_sha()


@router.get("/health")
async def health() -> Dict[str, Any]:
    """Basic health check endpoint.

    Returns:
        Health status response
    """
    return with_cid({"status": "ok"})


@router.get("/healthz")
async def healthz() -> Dict[str, Any]:
    """Health check with version info.

    Returns:
        Health status with version and build info
    """
    return with_cid({"ok": True, "version": APP_VERSION, "build": APP_BUILD})


@router.get("/readyz")
async def readyz() -> JSONResponse:
    """Readiness probe with Discogs connectivity check.

    Returns:
        Readiness status response
    """
    try:
        await run_in_threadpool(discogs_identity)
    except Exception as exc:
        payload = {
            "ok": False,
            "ready": False,
            "version": APP_VERSION,
            "build": APP_BUILD,
            "reason": str(exc),
        }
        return JSONResponse(status_code=503, content=with_cid(payload))
    return JSONResponse(
        status_code=200,
        content=with_cid({"ok": True, "ready": True, "version": APP_VERSION, "build": APP_BUILD}),
    )
