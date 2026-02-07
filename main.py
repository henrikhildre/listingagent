"""
FastAPI application for ListingAgent.

Provides REST endpoints for each phase of the listing workflow,
WebSocket support for real-time batch progress streaming, and
static file serving for the frontend.

Job state is filesystem-based at /tmp/jobs/{job_id}/:
    uploads/           - Raw uploaded files
    images/            - Extracted/copied images
    data_model.json    - Phase 1 output
    style_profile.json - Phase 2 output
    recipe.json        - Phase 3 output
    conversation.json  - Chat history
    output/            - Phase 4 output
        listings/      - Individual listing JSON files
        summary.csv    - All listings in one CSV
        report.json    - Batch execution report
    output.zip         - Downloadable archive
"""

import asyncio
import hashlib
import hmac
import json
import logging
import os
from pathlib import Path
from uuid import uuid4

from fastapi import (
    FastAPI,
    File,
    HTTPException,
    Request,
    UploadFile,
    WebSocket,
    WebSocketDisconnect,
)
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse, JSONResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel, Field

import calibration
import discovery
import executor
import recipe as recipe_module
from file_utils import create_job_directory, get_job_path

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# App setup
# ---------------------------------------------------------------------------

app = FastAPI(title="ListingAgent", version="0.1.0")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["https://listing.maybelater.no"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# ---------------------------------------------------------------------------
# Security headers
# ---------------------------------------------------------------------------


@app.middleware("http")
async def security_headers(request: Request, call_next):
    response = await call_next(request)
    response.headers["X-Content-Type-Options"] = "nosniff"
    response.headers["X-Frame-Options"] = "DENY"
    response.headers["Referrer-Policy"] = "strict-origin-when-cross-origin"
    response.headers["Strict-Transport-Security"] = "max-age=63072000; includeSubDomains"
    response.headers.pop("server", None)
    return response


# ---------------------------------------------------------------------------
# Auth — per-session tokens with rate limiting
# ---------------------------------------------------------------------------

APP_PASSWORD = os.getenv("APP_PASSWORD", "listingagent")
_active_tokens: set[str] = set()

# Rate limiting: track failed login attempts per IP
_login_attempts: dict[str, list[float]] = {}  # ip -> list of timestamps
_MAX_LOGIN_ATTEMPTS = 5
_LOGIN_WINDOW_SECONDS = 60

_PUBLIC_PATHS = frozenset({"/", "/api/login", "/api/auth-check"})


def _is_authenticated(request: Request) -> bool:
    token = request.cookies.get("session_token")
    return token is not None and token in _active_tokens


def _is_authenticated_cookie(cookies: dict) -> bool:
    """Check auth from a raw cookies dict (used by WebSocket)."""
    token = cookies.get("session_token")
    return token is not None and token in _active_tokens


def _check_rate_limit(ip: str) -> bool:
    """Return True if the IP is allowed to attempt login."""
    import time

    now = time.time()
    attempts = _login_attempts.get(ip, [])
    # Keep only attempts within the window
    attempts = [t for t in attempts if now - t < _LOGIN_WINDOW_SECONDS]
    _login_attempts[ip] = attempts
    return len(attempts) < _MAX_LOGIN_ATTEMPTS


def _record_failed_attempt(ip: str):
    import time

    _login_attempts.setdefault(ip, []).append(time.time())


@app.middleware("http")
async def auth_middleware(request: Request, call_next):
    path = request.url.path
    if (
        path in _PUBLIC_PATHS
        or path.startswith("/static/")
        or request.headers.get("upgrade", "").lower() == "websocket"
    ):
        return await call_next(request)

    if not _is_authenticated(request):
        return JSONResponse(status_code=401, content={"detail": "Not authenticated"})

    return await call_next(request)


class LoginRequest(BaseModel):
    password: str = Field(max_length=128)


@app.post("/api/login")
async def login(req: LoginRequest, request: Request):
    ip = request.client.host if request.client else "unknown"

    if not _check_rate_limit(ip):
        raise HTTPException(status_code=429, detail="Too many attempts. Try again later.")

    if not hmac.compare_digest(req.password, APP_PASSWORD):
        _record_failed_attempt(ip)
        raise HTTPException(status_code=401, detail="Wrong password")

    token = os.urandom(32).hex()
    _active_tokens.add(token)

    response = JSONResponse(content={"ok": True})
    response.set_cookie(
        key="session_token",
        value=token,
        httponly=True,
        secure=True,
        samesite="lax",
        max_age=60 * 60 * 24 * 30,  # 30 days
    )
    return response


@app.get("/api/auth-check")
async def auth_check(request: Request):
    if _is_authenticated(request):
        return {"authenticated": True}
    return JSONResponse(status_code=401, content={"detail": "Not authenticated"})


# ---------------------------------------------------------------------------
# Global state
# ---------------------------------------------------------------------------

# WebSocket connections per job_id
ws_connections: dict[str, set[WebSocket]] = {}

# Background execution tasks per job_id
active_tasks: dict[str, asyncio.Task] = {}

# ---------------------------------------------------------------------------
# Request / response models
# ---------------------------------------------------------------------------


class JobIdRequest(BaseModel):
    job_id: str


class ChatRequest(BaseModel):
    job_id: str
    message: str
    conversation_history: list[dict] = []


class BuildDataModelRequest(BaseModel):
    job_id: str
    conversation_history: list[dict] = []


class TestRecipeRequest(BaseModel):
    job_id: str
    sample_product_ids: list[str] | None = None


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

MAX_UPLOAD_SIZE_MB = int(os.getenv("MAX_UPLOAD_SIZE_MB", "50"))


def _job_exists(job_id: str) -> Path:
    """Return job path if the job directory exists, else raise 404."""
    job_path = get_job_path(job_id)
    if not job_path.exists():
        raise HTTPException(status_code=404, detail=f"Job {job_id} not found")
    return job_path


def _determine_phase(job_path: Path) -> str:
    """Determine the current phase by checking which artifacts exist."""
    if (job_path / "output" / "report.json").exists():
        return "complete"
    if job_path.joinpath("job_id") and any(
        task_id in active_tasks and not active_tasks[task_id].done()
        for task_id in [job_path.name]
    ):
        return "executing"
    if (job_path / "recipe.json").exists():
        r = json.loads((job_path / "recipe.json").read_text())
        if r.get("approved"):
            return "executing"
        if r.get("test_results"):
            return "testing"
        return "building_recipe"
    if (job_path / "style_profile.json").exists():
        return "building_recipe"
    if (job_path / "data_model.json").exists():
        return "interviewing"
    if (job_path / "uploads").exists() and any((job_path / "uploads").iterdir()):
        return "discovering"
    return "uploading"


def _load_json_artifact(job_path: Path, filename: str) -> dict | None:
    """Load a JSON file from the job directory, or return None."""
    filepath = job_path / filename
    if filepath.exists():
        return json.loads(filepath.read_text())
    return None


# ---------------------------------------------------------------------------
# 1. POST /api/upload
# ---------------------------------------------------------------------------


@app.post("/api/upload")
async def upload_files(files: list[UploadFile] = File(...)):
    """Accept file uploads, create job directory, save files. Return job_id + file list."""
    job_id = str(uuid4())
    job_path = create_job_directory(job_id)
    uploads_dir = job_path / "uploads"

    saved_files = []

    for upload in files:
        # Basic size guard (read into memory -- fine for hackathon scale)
        content = await upload.read()
        size_mb = len(content) / (1024 * 1024)
        if size_mb > MAX_UPLOAD_SIZE_MB:
            raise HTTPException(
                status_code=413,
                detail=f"File {upload.filename} exceeds {MAX_UPLOAD_SIZE_MB}MB limit",
            )

        # Sanitize filename — strip any directory components to prevent traversal
        raw_name = upload.filename or f"file_{len(saved_files)}"
        filename = Path(raw_name).name
        if not filename or filename.startswith("."):
            filename = f"file_{len(saved_files)}"
        dest = uploads_dir / filename

        dest.write_bytes(content)
        saved_files.append(filename)
        logger.info("Saved upload %s (%0.1f MB) for job %s", filename, size_mb, job_id)

    return {
        "job_id": job_id,
        "files": saved_files,
        "file_count": len(saved_files),
    }


# ---------------------------------------------------------------------------
# 2. POST /api/discover
# ---------------------------------------------------------------------------


@app.post("/api/discover")
async def discover(req: JobIdRequest):
    """Categorize uploads and run LLM-driven data exploration (Phase 1)."""
    job_path = _job_exists(req.job_id)

    try:
        file_summary = await discovery.categorize_uploads(req.job_id)
        analysis = await discovery.explore_data(req.job_id, file_summary)
    except Exception as e:
        logger.exception("Discovery failed for job %s", req.job_id)
        raise HTTPException(status_code=500, detail=str(e))

    return {
        "job_id": req.job_id,
        "file_summary": file_summary,
        "analysis": analysis,
    }


# ---------------------------------------------------------------------------
# 3. POST /api/chat
# ---------------------------------------------------------------------------


@app.post("/api/chat")
async def chat(req: ChatRequest):
    """
    Unified chat endpoint for Phases 2 and 3.

    Routes messages based on the current phase:
    - interviewing: calibration.start_interview() or calibration.process_message()
    - building_recipe / testing: recipe_module.refine_recipe()
    """
    job_path = _job_exists(req.job_id)
    phase = _determine_phase(job_path)

    try:
        # --- Phase 2: Interview ---
        if phase in ("discovering", "interviewing"):
            data_model = _load_json_artifact(job_path, "data_model.json")

            # First message after discovery -- kick off the interview
            if not req.conversation_history and data_model:
                response_text = await calibration.start_interview(
                    req.job_id, data_model
                )
                return {
                    "response": response_text,
                    "phase": "interviewing",
                    "style_profile": None,
                }

            # Ongoing interview
            result = await calibration.process_message(
                req.job_id,
                req.message,
                req.conversation_history,
            )
            return {
                "response": result["response"],
                "phase": result["phase"],
                "style_profile": result.get("style_profile"),
            }

        # --- Phase 3: Recipe refinement ---
        if phase in ("building_recipe", "testing"):
            current_recipe = _load_json_artifact(job_path, "recipe.json")
            if not current_recipe:
                raise HTTPException(
                    status_code=400,
                    detail="No recipe found. Call /api/test-recipe first to draft one.",
                )

            test_results = current_recipe.get("test_results", [])
            updated_recipe = await recipe_module.refine_recipe(
                req.job_id,
                current_recipe,
                req.message,
                test_results,
            )

            return {
                "response": updated_recipe.get(
                    "changes_made", "Recipe updated based on your feedback."
                ),
                "phase": "building_recipe",
                "recipe": updated_recipe,
            }

        # Phase doesn't support chat
        return {
            "response": f"Chat is not available in the current phase ({phase}).",
            "phase": phase,
        }

    except HTTPException:
        raise
    except Exception as e:
        logger.exception("Chat failed for job %s", req.job_id)
        raise HTTPException(status_code=500, detail=str(e))


# ---------------------------------------------------------------------------
# 4. POST /api/build-data-model
# ---------------------------------------------------------------------------


@app.post("/api/build-data-model")
async def build_data_model(req: BuildDataModelRequest):
    """Finalize the data model from the discovery conversation (Phase 1 -> 2 transition)."""
    job_path = _job_exists(req.job_id)

    try:
        data_model = await discovery.build_data_model(
            req.job_id, req.conversation_history
        )
    except Exception as e:
        logger.exception("build_data_model failed for job %s", req.job_id)
        raise HTTPException(status_code=500, detail=str(e))

    return {
        "job_id": req.job_id,
        "data_model": data_model,
        "product_count": len(data_model.get("products", [])),
    }


# ---------------------------------------------------------------------------
# 5. POST /api/test-recipe
# ---------------------------------------------------------------------------


@app.post("/api/test-recipe")
async def test_recipe_endpoint(req: TestRecipeRequest):
    """Draft a recipe if none exists, then test it on sample products."""
    job_path = _job_exists(req.job_id)

    try:
        # Load required artifacts
        style_profile = _load_json_artifact(job_path, "style_profile.json")
        data_model = _load_json_artifact(job_path, "data_model.json")

        if not style_profile:
            raise HTTPException(
                status_code=400,
                detail="Style profile not found. Complete the interview first.",
            )
        if not data_model:
            raise HTTPException(
                status_code=400,
                detail="Data model not found. Complete discovery first.",
            )

        # Draft recipe if it doesn't exist yet
        current_recipe = _load_json_artifact(job_path, "recipe.json")
        if not current_recipe:
            current_recipe = await recipe_module.draft_recipe(
                req.job_id, style_profile, data_model
            )

        # Test the recipe
        test_results = await recipe_module.test_recipe(
            req.job_id,
            current_recipe,
            sample_product_ids=req.sample_product_ids,
        )

        # Reload recipe (test_recipe saves updated results)
        current_recipe = _load_json_artifact(job_path, "recipe.json")

    except HTTPException:
        raise
    except Exception as e:
        logger.exception("test-recipe failed for job %s", req.job_id)
        raise HTTPException(status_code=500, detail=str(e))

    return {
        "job_id": req.job_id,
        "recipe": current_recipe,
        "test_results": test_results,
    }


# ---------------------------------------------------------------------------
# 6. POST /api/approve-recipe
# ---------------------------------------------------------------------------


@app.post("/api/approve-recipe")
async def approve_recipe_endpoint(req: JobIdRequest):
    """Lock the recipe for batch execution."""
    job_path = _job_exists(req.job_id)

    current_recipe = _load_json_artifact(job_path, "recipe.json")
    if not current_recipe:
        raise HTTPException(status_code=400, detail="No recipe found to approve.")

    try:
        approved = await recipe_module.approve_recipe(req.job_id, current_recipe)
    except Exception as e:
        logger.exception("approve-recipe failed for job %s", req.job_id)
        raise HTTPException(status_code=500, detail=str(e))

    return {
        "job_id": req.job_id,
        "recipe": approved,
        "approved": True,
    }


# ---------------------------------------------------------------------------
# 7. POST /api/execute
# ---------------------------------------------------------------------------


@app.post("/api/execute")
async def execute(req: JobIdRequest):
    """Start batch execution as a background task. Returns immediately."""
    job_path = _job_exists(req.job_id)

    # Check prerequisites
    current_recipe = _load_json_artifact(job_path, "recipe.json")
    if not current_recipe:
        raise HTTPException(
            status_code=400, detail="No recipe found. Build and test one first."
        )

    if not current_recipe.get("approved"):
        raise HTTPException(
            status_code=400, detail="Recipe must be approved before execution."
        )

    # Prevent duplicate execution
    if req.job_id in active_tasks and not active_tasks[req.job_id].done():
        raise HTTPException(status_code=409, detail="Execution already in progress.")

    # Get or create connection set for this job
    connections = ws_connections.setdefault(req.job_id, set())

    # Launch background task
    task = asyncio.create_task(_run_batch(req.job_id, connections))
    active_tasks[req.job_id] = task

    logger.info("Batch execution started for job %s", req.job_id)

    return {
        "job_id": req.job_id,
        "status": "started",
    }


async def _run_batch(job_id: str, connections: set):
    """Wrapper around executor.execute_batch with error handling."""
    try:
        report = await executor.execute_batch(job_id, connections)
        logger.info("Batch complete for job %s: %s", job_id, report)
    except Exception as e:
        logger.exception("Batch execution failed for job %s", job_id)
        # Notify connected clients of the failure
        try:
            payload = json.dumps(
                {
                    "type": "batch_error",
                    "job_id": job_id,
                    "error": str(e),
                }
            )
            dead = set()
            for ws in connections:
                try:
                    await ws.send_text(payload)
                except Exception:
                    dead.add(ws)
            connections -= dead
        except Exception:
            pass


# ---------------------------------------------------------------------------
# 8. GET /api/status/{job_id}
# ---------------------------------------------------------------------------


@app.get("/api/status/{job_id}")
async def get_status(job_id: str):
    """Return current job status based on which artifacts exist."""
    job_path = _job_exists(job_id)
    phase = _determine_phase(job_path)

    response = {
        "job_id": job_id,
        "phase": phase,
    }

    # Include report stats if execution is complete
    report = _load_json_artifact(job_path, "output/report.json")
    if report:
        response["report"] = report

    # Include product count from data model
    data_model = _load_json_artifact(job_path, "data_model.json")
    if data_model:
        response["product_count"] = len(data_model.get("products", []))

    # Include recipe version / approval status
    current_recipe = _load_json_artifact(job_path, "recipe.json")
    if current_recipe:
        response["recipe_version"] = current_recipe.get("version")
        response["recipe_approved"] = current_recipe.get("approved", False)

    return response


# ---------------------------------------------------------------------------
# 9. GET /api/download/{job_id}
# ---------------------------------------------------------------------------


@app.get("/api/download/{job_id}")
async def download(job_id: str):
    """Return the output.zip for a completed job."""
    job_path = _job_exists(job_id)

    zip_path = job_path / "output.zip"
    if not zip_path.exists():
        raise HTTPException(
            status_code=404,
            detail="Output ZIP not found. Batch execution may not be complete.",
        )

    return FileResponse(
        path=str(zip_path),
        media_type="application/zip",
        filename=f"listings_{job_id[:8]}.zip",
    )


# ---------------------------------------------------------------------------
# 10. WebSocket /ws/{job_id}
# ---------------------------------------------------------------------------


@app.websocket("/ws/{job_id}")
async def websocket_endpoint(websocket: WebSocket, job_id: str):
    """Accept WebSocket connection and keep alive for progress streaming."""
    if not _is_authenticated_cookie(websocket.cookies):
        await websocket.close(code=4401, reason="Not authenticated")
        return
    await websocket.accept()

    # Register connection
    connections = ws_connections.setdefault(job_id, set())
    connections.add(websocket)
    logger.info("WebSocket connected for job %s (total: %d)", job_id, len(connections))

    try:
        # Keep connection alive -- listen for client messages (pings, etc.)
        while True:
            # await incoming messages to detect disconnects
            await websocket.receive_text()
    except WebSocketDisconnect:
        pass
    except Exception:
        pass
    finally:
        connections.discard(websocket)
        logger.info(
            "WebSocket disconnected for job %s (remaining: %d)",
            job_id,
            len(connections),
        )
        # Clean up empty sets
        if not connections:
            ws_connections.pop(job_id, None)


# ---------------------------------------------------------------------------
# Static files & root
# ---------------------------------------------------------------------------

# Mount static files (must be after all routes to avoid shadowing)
static_dir = Path(__file__).parent / "static"
if static_dir.exists():
    app.mount("/static", StaticFiles(directory=str(static_dir)), name="static")


@app.get("/")
async def root():
    """Serve the frontend SPA."""
    index_path = static_dir / "index.html"
    if index_path.exists():
        return FileResponse(str(index_path))
    return {
        "message": "ListingAgent API is running. Frontend not found at static/index.html."
    }
