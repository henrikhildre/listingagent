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
import hmac
import json
import logging
import os
import shutil
import time
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
from fastapi.responses import FileResponse, JSONResponse, StreamingResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel, Field

import calibration
import discovery
import executor
import pipeline_cache
import recipe as recipe_module
from file_utils import (
    IMAGE_EXTENSIONS,
    MAX_PASTE_LENGTH,
    create_job_directory,
    get_job_path,
    save_pasted_text,
)

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
    response.headers["Strict-Transport-Security"] = (
        "max-age=63072000; includeSubDomains"
    )
    if "server" in response.headers:
        del response.headers["server"]
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
_PUBLIC_PREFIXES = ("/api/demo-catalog", "/api/demo-image/")


def _is_authenticated(request: Request) -> bool:
    token = request.cookies.get("session_token")
    return token is not None and token in _active_tokens


def _is_authenticated_cookie(cookies: dict) -> bool:
    """Check auth from a raw cookies dict (used by WebSocket)."""
    token = cookies.get("session_token")
    return token is not None and token in _active_tokens


def _check_rate_limit(ip: str) -> bool:
    """Return True if the IP is allowed to attempt login."""
    now = time.time()
    attempts = _login_attempts.get(ip, [])
    # Keep only attempts within the window
    attempts = [t for t in attempts if now - t < _LOGIN_WINDOW_SECONDS]
    _login_attempts[ip] = attempts
    return len(attempts) < _MAX_LOGIN_ATTEMPTS


def _record_failed_attempt(ip: str):
    _login_attempts.setdefault(ip, []).append(time.time())


@app.middleware("http")
async def auth_middleware(request: Request, call_next):
    path = request.url.path
    if (
        path in _PUBLIC_PATHS
        or path.startswith("/static/")
        or any(path.startswith(p) for p in _PUBLIC_PREFIXES)
        or request.headers.get("upgrade", "").lower() == "websocket"
    ):
        return await call_next(request)

    if not _is_authenticated(request):
        return JSONResponse(status_code=401, content={"detail": "Not authenticated"})

    return await call_next(request)


class LoginRequest(BaseModel):
    password: str = Field(max_length=128)
    username: str = Field(default="", max_length=64)


@app.post("/api/login")
async def login(req: LoginRequest, request: Request):
    ip = request.client.host if request.client else "unknown"

    if not _check_rate_limit(ip):
        raise HTTPException(
            status_code=429, detail="Too many attempts. Try again later."
        )

    if not hmac.compare_digest(req.password, APP_PASSWORD):
        _record_failed_attempt(ip)
        raise HTTPException(status_code=401, detail="Wrong password")

    token = os.urandom(32).hex()
    _active_tokens.add(token)

    username = req.username.strip() or "anonymous"

    response = JSONResponse(content={"ok": True, "username": username})
    response.set_cookie(
        key="session_token",
        value=token,
        httponly=True,
        secure=True,
        samesite="lax",
        max_age=60 * 60 * 24 * 30,  # 30 days
    )
    response.set_cookie(
        key="session_username",
        value=username,
        httponly=False,
        secure=True,
        samesite="lax",
        max_age=60 * 60 * 24 * 30,
    )
    return response


@app.get("/api/auth-check")
async def auth_check(request: Request):
    if _is_authenticated(request):
        username = request.cookies.get("session_username", "anonymous")
        return {"authenticated": True, "username": username}
    return JSONResponse(status_code=401, content={"detail": "Not authenticated"})


@app.get("/api/token-usage")
async def token_usage():
    from gemini_client import get_token_usage
    return get_token_usage()


@app.post("/api/token-usage/reset")
async def token_usage_reset():
    from gemini_client import reset_token_usage
    reset_token_usage()
    return {"status": "reset"}


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


class PasteTextRequest(BaseModel):
    text: str = Field(max_length=MAX_PASTE_LENGTH)


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
    job_id = job_path.name
    if job_id in active_tasks and not active_tasks[job_id].done():
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
async def upload_files(request: Request, files: list[UploadFile] = File(...)):
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

    # Check fingerprint + cache
    result = {
        "job_id": job_id,
        "files": saved_files,
        "file_count": len(saved_files),
    }

    username = request.cookies.get("session_username", "anonymous")
    try:
        fp_result = pipeline_cache.compute_fingerprint_for_job(job_id)
        if fp_result:
            fingerprint, headers = fp_result
            result["fingerprint"] = fingerprint
            cache_meta = pipeline_cache.lookup_cache(username, fingerprint)
            if cache_meta:
                result["cache_hit"] = True
                result["cache_meta"] = cache_meta
    except Exception as e:
        logger.warning("Fingerprint/cache check failed for job %s: %s", job_id, e)

    return result


# ---------------------------------------------------------------------------
# 1b. Demo data endpoints
# ---------------------------------------------------------------------------

DEMO_DATA_DIR = Path(__file__).parent / "demo_data"

@app.get("/api/demo-catalog")
async def demo_catalog():
    """Return the demo manifest enriched with preview text for paste/JSON demos."""
    manifest_path = DEMO_DATA_DIR / "manifest.json"
    if not manifest_path.exists():
        raise HTTPException(status_code=404, detail="Demo manifest not found.")

    manifest = json.loads(manifest_path.read_text())
    demos = manifest.get("demos", [])

    for demo in demos:
        demo_dir = DEMO_DATA_DIR / demo["id"]
        method = demo.get("input_method")
        files = demo.get("files", {})

        if method == "paste_text" and files.get("text_file"):
            text_path = demo_dir / files["text_file"]
            if text_path.exists():
                demo["preview_text"] = text_path.read_text(encoding="utf-8")[:500]

        elif method == "json" and files.get("json_file"):
            json_path = demo_dir / files["json_file"]
            if json_path.exists():
                demo["preview_text"] = json_path.read_text(encoding="utf-8")[:500]

        elif method == "images_only":
            images_dir = demo_dir / files.get("images_dir", "images")
            if images_dir.exists():
                demo["all_images"] = sorted(
                    f.name for f in images_dir.iterdir()
                    if f.is_file() and f.suffix.lower() in IMAGE_EXTENSIONS
                )

    return {"demos": demos}


@app.get("/api/demo-image/{demo_id}/{filename}")
async def demo_image(demo_id: str, filename: str):
    """Serve a demo image for thumbnail previews. Path-traversal-safe."""
    # Sanitize inputs
    safe_demo_id = Path(demo_id).name
    safe_filename = Path(filename).name
    if safe_demo_id != demo_id or safe_filename != filename:
        raise HTTPException(status_code=400, detail="Invalid path.")

    image_path = DEMO_DATA_DIR / safe_demo_id / "images" / safe_filename
    if not image_path.exists() or not image_path.is_file():
        raise HTTPException(status_code=404, detail="Image not found.")

    return FileResponse(str(image_path))


class LoadDemoRequest(BaseModel):
    demo_id: str = "vintage_inventory"


@app.post("/api/load-demo")
async def load_demo(request: Request, req: LoadDemoRequest = LoadDemoRequest()):
    """Load a bundled demo dataset into a new job. Returns same shape as /api/upload."""
    demo_dir = DEMO_DATA_DIR / req.demo_id
    if not demo_dir.exists():
        raise HTTPException(status_code=404, detail="Demo data not found on server.")

    # Load manifest to get demo config
    manifest_path = DEMO_DATA_DIR / "manifest.json"
    manifest = json.loads(manifest_path.read_text())
    demo_config = next(
        (d for d in manifest.get("demos", []) if d["id"] == req.demo_id), None
    )
    if not demo_config:
        raise HTTPException(status_code=404, detail=f"Demo '{req.demo_id}' not found.")

    job_id = str(uuid4())
    job_path = create_job_directory(job_id)
    uploads_dir = job_path / "uploads"

    method = demo_config.get("input_method")
    files = demo_config.get("files", {})
    saved_files = []

    if method == "file_upload":
        # Copy spreadsheet
        spreadsheet = files.get("spreadsheet")
        if spreadsheet:
            src = demo_dir / spreadsheet
            if src.exists():
                shutil.copy2(src, uploads_dir / src.name)
                saved_files.append(src.name)
        # Copy images
        images_dir = demo_dir / files.get("images_dir", "images")
        if images_dir.exists():
            for f in sorted(images_dir.iterdir()):
                if f.is_file() and f.suffix.lower() in IMAGE_EXTENSIONS:
                    shutil.copy2(f, uploads_dir / f.name)
                    saved_files.append(f.name)

    elif method == "paste_text":
        text_file = files.get("text_file")
        if text_file:
            text = (demo_dir / text_file).read_text(encoding="utf-8")
            save_pasted_text(job_id, text)

    elif method == "json":
        json_file = files.get("json_file")
        if json_file:
            src = demo_dir / json_file
            if src.exists():
                shutil.copy2(src, uploads_dir / src.name)
                saved_files.append(src.name)

    elif method == "images_only":
        images_dir = demo_dir / files.get("images_dir", "images")
        if images_dir.exists():
            for f in sorted(images_dir.iterdir()):
                if f.is_file() and f.suffix.lower() in IMAGE_EXTENSIONS:
                    shutil.copy2(f, uploads_dir / f.name)
                    saved_files.append(f.name)

    logger.info("Loaded demo '%s' for job %s: %d files", req.demo_id, job_id, len(saved_files))

    result = {
        "job_id": job_id,
        "files": saved_files,
        "file_count": len(saved_files),
    }

    username = request.cookies.get("session_username", "anonymous")
    try:
        fp_result = pipeline_cache.compute_fingerprint_for_job(job_id)
        if fp_result:
            fingerprint, headers = fp_result
            result["fingerprint"] = fingerprint
            cache_meta = pipeline_cache.lookup_cache(username, fingerprint)
            if cache_meta:
                result["cache_hit"] = True
                result["cache_meta"] = cache_meta
    except Exception as e:
        logger.warning("Fingerprint/cache check failed for demo job %s: %s", job_id, e)

    return result


# ---------------------------------------------------------------------------
# 1c. POST /api/paste
# ---------------------------------------------------------------------------


@app.post("/api/paste")
async def paste_text(req: PasteTextRequest):
    """Accept pasted text input, create job directory, save text. Return job_id."""
    text = req.text.strip()
    if not text:
        raise HTTPException(status_code=400, detail="No text provided.")

    if len(text) > MAX_PASTE_LENGTH:
        raise HTTPException(
            status_code=413,
            detail=f"Text exceeds {MAX_PASTE_LENGTH:,} character limit.",
        )

    job_id = str(uuid4())
    job_path = create_job_directory(job_id)

    save_pasted_text(job_id, text)

    logger.info(
        "Saved pasted text for job %s (%d characters)", job_id, len(text),
    )

    return {
        "job_id": job_id,
        "text_length": len(text),
    }


# ---------------------------------------------------------------------------
# 1d. GET /api/preview-data/{job_id} — inspect uploaded data before discovery
# ---------------------------------------------------------------------------


@app.get("/api/preview-data/{job_id}")
async def preview_data(job_id: str):
    """Return a preview of uploaded files so the user can inspect before processing."""
    _job_exists(job_id)
    file_summary = await discovery.categorize_uploads(job_id)
    return {"job_id": job_id, "preview": file_summary}


@app.get("/api/job-image/{job_id}/{filename}")
async def job_image(job_id: str, filename: str):
    """Serve an uploaded image from a job's uploads directory."""
    job_path = _job_exists(job_id)
    safe_filename = Path(filename).name
    if safe_filename != filename:
        raise HTTPException(status_code=400, detail="Invalid filename.")
    image_path = job_path / "uploads" / safe_filename
    if not image_path.exists():
        raise HTTPException(status_code=404, detail="Image not found.")
    return FileResponse(str(image_path))


# ---------------------------------------------------------------------------
# 2. POST /api/discover
# ---------------------------------------------------------------------------


@app.post("/api/discover")
async def discover(req: JobIdRequest):
    """Categorize uploads and run LLM-driven data exploration (Phase 1)."""
    from gemini_client import start_step, end_step
    _job_exists(req.job_id)

    start_step("discover")
    try:
        file_summary = await discovery.categorize_uploads(req.job_id)
        analysis = await discovery.explore_data(req.job_id, file_summary)
    except Exception as e:
        logger.exception("Discovery failed for job %s", req.job_id)
        raise HTTPException(status_code=500, detail=str(e))

    # Check if cached artifacts already exist in this job
    job_path = get_job_path(req.job_id)
    has_style_profile = (job_path / "style_profile.json").exists()
    has_approved_recipe = False
    recipe_data = _load_json_artifact(job_path, "recipe.json")
    if recipe_data and recipe_data.get("approved"):
        has_approved_recipe = True

    end_step("discover")

    return {
        "job_id": req.job_id,
        "response": analysis,
        "categories": file_summary,
        "has_style_profile": has_style_profile,
        "has_approved_recipe": has_approved_recipe,
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

    from gemini_client import start_step, end_step

    try:
        # --- Phase 2: Interview ---
        if phase in ("discovering", "interviewing"):
            data_model = _load_json_artifact(job_path, "data_model.json")

            # First message after discovery -- kick off the interview
            if not req.conversation_history and data_model:
                start_step("interview")
                response_text = await calibration.start_interview(
                    req.job_id, data_model
                )
                end_step("interview")
                return {
                    "response": response_text,
                    "phase": "interviewing",
                    "style_profile": None,
                }

            # Ongoing interview
            start_step("interview")
            result = await calibration.process_message(
                req.job_id,
                req.message,
                req.conversation_history,
            )
            step_info = end_step("interview")
            # If the profile is ready, this was the final interview step
            if result["phase"] == "profile_ready":
                logger.info("Interview complete — total interview cost included in step history")
            return {
                "response": result["response"],
                "phase": result["phase"],
                "style_profile": result.get("style_profile"),
            }

        # --- Phase 3: Recipe refinement ---
        if phase in ("building_recipe", "testing"):
            current_recipe = _load_json_artifact(job_path, "recipe.json")
            if not current_recipe:
                return {
                    "response": "Your style profile is ready! Let me build a listing recipe now.",
                    "phase": "start_recipe",
                }

            start_step("recipe_refine")
            test_results = current_recipe.get("test_results", [])
            updated_recipe = await recipe_module.refine_recipe(
                req.job_id,
                current_recipe,
                req.message,
                test_results,
            )
            end_step("recipe_refine")

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
    """Finalize the data model from the discovery conversation (Phase 1 -> 2 transition).

    Streams progress via Server-Sent Events, then emits a complete or error event.
    """
    _job_exists(req.job_id)

    queue: asyncio.Queue = asyncio.Queue()

    async def on_progress(text: str):
        await queue.put(_sse_event("progress", {"text": text}))

    async def run_build():
        from gemini_client import start_step, end_step
        start_step("build_data_model")
        try:
            data_model = await discovery.build_data_model(
                req.job_id, req.conversation_history, progress=on_progress,
            )
            step_summary = end_step("build_data_model")
            await queue.put(_sse_event("complete", {
                "job_id": req.job_id,
                "data_model": data_model,
                "product_count": len(data_model.get("products", [])),
                "quality_report": data_model.get("quality_report", {}),
                "token_cost": step_summary,
            }))
        except Exception as e:
            end_step("build_data_model")
            logger.exception("build_data_model failed for job %s", req.job_id)
            await queue.put(_sse_event("error", {"text": str(e)}))
        finally:
            await queue.put(None)  # sentinel

    async def event_stream():
        task = asyncio.create_task(run_build())
        try:
            while True:
                try:
                    item = await asyncio.wait_for(queue.get(), timeout=0.5)
                except asyncio.TimeoutError:
                    continue
                if item is None:
                    break
                yield item
        finally:
            if not task.done():
                task.cancel()

    return StreamingResponse(
        event_stream(),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "X-Accel-Buffering": "no",
        },
    )


# ---------------------------------------------------------------------------
# 5. POST /api/test-recipe
# ---------------------------------------------------------------------------


@app.post("/api/test-recipe")
async def test_recipe_endpoint(req: TestRecipeRequest):
    """Draft a recipe if none exists, then test it on sample products."""
    from gemini_client import start_step, end_step, estimate_batch_cost
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
            start_step("draft_recipe")
            current_recipe = await recipe_module.draft_recipe(
                req.job_id, style_profile, data_model
            )
            end_step("draft_recipe")

        # Test the recipe
        start_step("test_recipe")
        test_results = await recipe_module.test_recipe(
            req.job_id,
            current_recipe,
            sample_product_ids=req.sample_product_ids,
        )
        step_info = end_step("test_recipe")

        # Reload recipe (test_recipe saves updated results)
        current_recipe = _load_json_artifact(job_path, "recipe.json")

        # Project full batch cost from sample
        total_products = len(data_model.get("products", []))
        sample_count = len(test_results)
        cost_estimate = estimate_batch_cost(sample_count, total_products)

    except HTTPException:
        raise
    except Exception as e:
        logger.exception("test-recipe failed for job %s", req.job_id)
        raise HTTPException(status_code=500, detail=str(e))

    return {
        "job_id": req.job_id,
        "recipe": current_recipe,
        "test_results": test_results,
        "cost_estimate": cost_estimate,
    }


# ---------------------------------------------------------------------------
# 5b. POST /api/auto-refine (SSE)
# ---------------------------------------------------------------------------


def _sse_event(event_type: str, data: dict) -> str:
    """Format a Server-Sent Event."""
    return f"event: {event_type}\ndata: {json.dumps(data, default=str)}\n\n"


@app.post("/api/auto-refine")
async def auto_refine(req: JobIdRequest):
    """
    Draft, test, and auto-refine the recipe in a loop.
    Streams progress via Server-Sent Events.
    """
    job_path = _job_exists(req.job_id)

    async def event_stream():
        from gemini_client import start_step, end_step, estimate_batch_cost
        try:
            style_profile = _load_json_artifact(job_path, "style_profile.json")
            data_model = _load_json_artifact(job_path, "data_model.json")

            if not style_profile or not data_model:
                yield _sse_event(
                    "error", {"text": "Missing style profile or data model."}
                )
                return

            total_products = len(data_model.get("products", []))

            # 1. Draft recipe if needed
            current_recipe = _load_json_artifact(job_path, "recipe.json")
            if not current_recipe:
                yield _sse_event("progress", {"text": "Drafting listing template..."})
                start_step("draft_recipe")
                current_recipe = await recipe_module.draft_recipe(
                    req.job_id, style_profile, data_model
                )
                end_step("draft_recipe")

            # 2. Initial test
            yield _sse_event("progress", {"text": "Testing on sample products..."})
            start_step("test_recipe")
            test_results = await recipe_module.test_recipe(req.job_id, current_recipe)
            end_step("test_recipe")
            current_recipe = _load_json_artifact(job_path, "recipe.json")

            def check_quality(results):
                """Check if test results meet quality threshold."""
                avg = _calc_avg_score(results)
                all_passed = all(
                    tr.get("validation", {}).get("passed", False) for tr in results
                )
                return avg, all_passed

            avg, all_passed = check_quality(test_results)
            yield _sse_event(
                "score",
                {
                    "attempt": 1,
                    "avg": avg,
                    "all_passed": all_passed,
                    "details": _summarize_results(test_results),
                },
            )

            # 3. Auto-refine loop (up to 3 iterations)
            iterations = 1
            for i in range(3):
                if avg >= 90 and all_passed:
                    break

                feedback = recipe_module.build_auto_feedback(test_results)
                yield _sse_event(
                    "progress",
                    {"text": f"Some listings need work — improving the recipe (round {i + 2})..."},
                )

                start_step(f"refine_{i + 2}")
                current_recipe = await recipe_module.refine_recipe(
                    req.job_id, current_recipe, feedback, test_results
                )
                end_step(f"refine_{i + 2}")

                changes = current_recipe.get("changes_made", "")
                if changes:
                    yield _sse_event(
                        "progress",
                        {"text": f"{changes} — re-testing (round {i + 2})..."},
                    )
                else:
                    yield _sse_event(
                        "progress",
                        {"text": f"Recipe improved — re-testing (round {i + 2})..."},
                    )
                start_step(f"retest_{i + 2}")
                test_results = await recipe_module.test_recipe(
                    req.job_id, current_recipe
                )
                end_step(f"retest_{i + 2}")
                current_recipe = _load_json_artifact(job_path, "recipe.json")

                avg, all_passed = check_quality(test_results)
                iterations = i + 2

                yield _sse_event(
                    "score",
                    {
                        "attempt": iterations,
                        "avg": avg,
                        "all_passed": all_passed,
                        "details": _summarize_results(test_results),
                    },
                )

            # 4. Final result
            reached = avg >= 90 and all_passed
            remaining_issues = (
                [
                    {
                        "product": tr.get("product_name") or tr.get("product_id"),
                        "issues": issues,
                    }
                    for tr in test_results
                    if (issues := tr.get("validation", {}).get("issues", []))
                ]
                if not reached
                else []
            )

            # Project batch cost from samples
            sample_count = len(test_results)
            cost_estimate = estimate_batch_cost(sample_count, total_products)

            yield _sse_event(
                "complete",
                {
                    "test_results": test_results,
                    "recipe": current_recipe,
                    "reached_threshold": reached,
                    "iterations": iterations,
                    "avg_score": avg,
                    "remaining_issues": remaining_issues,
                    "cost_estimate": cost_estimate,
                },
            )

        except Exception as e:
            logger.exception("auto-refine failed for job %s", req.job_id)
            yield _sse_event("error", {"text": str(e)})

    return StreamingResponse(
        event_stream(),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "X-Accel-Buffering": "no",
        },
    )


def _calc_avg_score(test_results: list[dict]) -> int:
    """Calculate average validation score from test results."""
    if not test_results:
        return 0
    return round(
        sum(tr.get("validation", {}).get("score", 0) for tr in test_results)
        / len(test_results)
    )


def _summarize_results(test_results: list[dict]) -> list[dict]:
    """Create a brief summary of each test result."""
    return [
        {
            "product": tr.get("product_name") or tr.get("product_id", "?"),
            "score": tr.get("validation", {}).get("score", 0),
            "passed": tr.get("validation", {}).get("passed", False),
            "issues": tr.get("validation", {}).get("issues", []),
        }
        for tr in test_results
    ]


# ---------------------------------------------------------------------------
# 6. POST /api/approve-recipe
# ---------------------------------------------------------------------------


@app.post("/api/approve-recipe")
async def approve_recipe_endpoint(req: JobIdRequest, request: Request):
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

    # Save to pipeline cache (best-effort, never blocks approval)
    try:
        username = request.cookies.get("session_username", "anonymous")
        extraction_script = _load_json_artifact(job_path, "extraction_script.json")
        if extraction_script:
            fingerprint = extraction_script.get("fingerprint")
            headers = extraction_script.get("headers", [])
            if fingerprint and headers:
                pipeline_cache.save_to_cache(username, fingerprint, req.job_id, headers)
    except Exception as e:
        logger.warning("Cache save failed for job %s: %s", req.job_id, e)

    return {
        "job_id": req.job_id,
        "recipe": approved,
        "approved": True,
    }


# ---------------------------------------------------------------------------
# 6b. POST /api/apply-cache
# ---------------------------------------------------------------------------


class ApplyCacheRequest(BaseModel):
    job_id: str
    fingerprint: str
    mode: str = Field(pattern=r"^(full_reuse|adjust_style|fresh)$")


@app.post("/api/apply-cache")
async def apply_cache(req: ApplyCacheRequest, request: Request):
    """Apply cached pipeline artifacts to a job."""
    _job_exists(req.job_id)
    username = request.cookies.get("session_username", "anonymous")

    try:
        pipeline_cache.apply_cache_to_job(username, req.fingerprint, req.job_id, req.mode)
    except FileNotFoundError:
        raise HTTPException(status_code=404, detail="Cache not found.")
    except Exception as e:
        logger.exception("apply-cache failed for job %s", req.job_id)
        raise HTTPException(status_code=500, detail=str(e))

    return {"applied": True, "mode": req.mode}


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
    from gemini_client import start_step, end_step
    start_step("batch_execute")
    try:
        report = await executor.execute_batch(job_id, connections)
        step_summary = end_step("batch_execute")
        report["token_cost"] = step_summary
        logger.info("Batch complete for job %s: %s", job_id, report)
    except Exception as e:
        end_step("batch_execute")
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
# 9b. GET /api/download/{job_id}/{format}  — platform-specific exports
# ---------------------------------------------------------------------------

_EXPORT_FORMATS = {
    "etsy": ("etsy_upload.csv", "text/csv", "etsy_upload.csv"),
    "ebay": ("ebay_upload.csv", "text/csv", "ebay_upload.csv"),
    "shopify": ("shopify_upload.csv", "text/csv", "shopify_upload.csv"),
    "csv": ("summary.csv", "text/csv", "summary.csv"),
    "text": ("listings_copy_paste.txt", "text/plain", "listings.txt"),
}


@app.get("/api/download/{job_id}/{export_format}")
async def download_format(job_id: str, export_format: str):
    """Download a platform-specific export file."""
    job_path = _job_exists(job_id)

    fmt = _EXPORT_FORMATS.get(export_format)
    if not fmt:
        raise HTTPException(
            status_code=400,
            detail=f"Unknown format '{export_format}'. "
                   f"Available: {', '.join(_EXPORT_FORMATS.keys())}",
        )

    filename, media_type, download_name = fmt
    file_path = job_path / "output" / filename

    if not file_path.exists():
        # Try to generate on-the-fly if it doesn't exist
        try:
            from executor import (
                generate_etsy_csv,
                generate_ebay_csv,
                generate_shopify_csv,
                generate_copy_paste_text,
            )
            generators = {
                "etsy": generate_etsy_csv,
                "ebay": generate_ebay_csv,
                "shopify": generate_shopify_csv,
                "text": generate_copy_paste_text,
            }
            gen = generators.get(export_format)
            if gen:
                gen(job_id)
        except Exception as e:
            logger.warning("On-the-fly generation failed for %s: %s", export_format, e)

    if not file_path.exists():
        raise HTTPException(
            status_code=404,
            detail=f"Export file not found. Run batch execution first.",
        )

    return FileResponse(
        path=str(file_path),
        media_type=media_type,
        filename=f"{job_id[:8]}_{download_name}",
    )


# ---------------------------------------------------------------------------
# 9c. GET /api/listings/{job_id}  — all listings as JSON for frontend
# ---------------------------------------------------------------------------


@app.get("/api/listings/{job_id}")
async def get_listings(job_id: str):
    """Return all generated listings as a JSON array for the frontend."""
    job_path = _job_exists(job_id)
    listings_dir = job_path / "output" / "listings"

    if not listings_dir.exists():
        raise HTTPException(status_code=404, detail="No listings found.")

    listings = []
    for p in sorted(listings_dir.glob("*.json")):
        try:
            data = json.loads(p.read_text())
            listings.append(data)
        except Exception:
            pass

    return {"job_id": job_id, "listings": listings, "count": len(listings)}


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
