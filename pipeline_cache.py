"""
Pipeline cache — reuse discovery/interview/recipe artifacts across jobs
when the uploaded data has the same column structure.

Cache layout:
    data/_cache/{username}/{fingerprint}/
        meta.json             — metadata about the cached pipeline
        extraction_script.json
        style_profile.json
        recipe.json
"""

import json
import logging
import shutil
from datetime import datetime, timezone
from pathlib import Path

from discovery import _column_fingerprint
from file_utils import (
    categorize_files,
    get_job_path,
    read_json_sample,
    read_spreadsheet_preview,
)

logger = logging.getLogger(__name__)

CACHE_ROOT = Path(__file__).parent / "data" / "_cache"


def compute_fingerprint_for_job(job_id: str) -> tuple[str, list[str]] | None:
    """Compute a column fingerprint from the first spreadsheet or JSON in the job.

    Returns (fingerprint, headers) or None if no structured data found.
    """
    categories = categorize_files(job_id)
    uploads_dir = get_job_path(job_id) / "uploads"

    # Try spreadsheets first
    for filename in categories.get("spreadsheets", []):
        try:
            preview = read_spreadsheet_preview(uploads_dir / filename, max_rows=1)
            headers = preview.get("headers", [])
            if headers:
                fp = _column_fingerprint(headers)
                return fp, headers
        except Exception as e:
            logger.warning("Failed to read spreadsheet %s for fingerprint: %s", filename, e)

    # Try JSON files
    for filename in categories.get("json_files", []):
        try:
            sample = read_json_sample(uploads_dir / filename, max_sample=1)
            headers = sample.get("headers", [])
            if headers:
                fp = _column_fingerprint(headers)
                return fp, headers
        except Exception as e:
            logger.warning("Failed to read JSON %s for fingerprint: %s", filename, e)

    return None


def _cache_dir(username: str, fingerprint: str) -> Path:
    """Return the cache directory for a user + fingerprint combo."""
    # Sanitize username to prevent path traversal
    safe_user = "".join(c for c in username if c.isalnum() or c in "-_ ").strip() or "anonymous"
    return CACHE_ROOT / safe_user / fingerprint


def lookup_cache(username: str, fingerprint: str) -> dict | None:
    """Check if a cached pipeline exists for this user + fingerprint.

    Returns the meta.json contents or None.
    """
    meta_path = _cache_dir(username, fingerprint) / "meta.json"
    if not meta_path.exists():
        return None
    try:
        return json.loads(meta_path.read_text())
    except (json.JSONDecodeError, OSError) as e:
        logger.warning("Failed to read cache meta at %s: %s", meta_path, e)
        return None


def save_to_cache(username: str, fingerprint: str, job_id: str, headers: list[str]):
    """Copy pipeline artifacts from a job into the cache."""
    job_path = get_job_path(job_id)
    cache_path = _cache_dir(username, fingerprint)
    cache_path.mkdir(parents=True, exist_ok=True)

    # Copy artifacts
    for artifact in ("extraction_script.json", "style_profile.json", "recipe.json"):
        src = job_path / artifact
        if src.exists():
            shutil.copy2(src, cache_path / artifact)

    # Build metadata
    style_profile = {}
    sp_path = job_path / "style_profile.json"
    if sp_path.exists():
        try:
            style_profile = json.loads(sp_path.read_text())
        except (json.JSONDecodeError, OSError):
            pass

    data_model = {}
    dm_path = job_path / "data_model.json"
    if dm_path.exists():
        try:
            data_model = json.loads(dm_path.read_text())
        except (json.JSONDecodeError, OSError):
            pass

    recipe = {}
    rp_path = job_path / "recipe.json"
    if rp_path.exists():
        try:
            recipe = json.loads(rp_path.read_text())
        except (json.JSONDecodeError, OSError):
            pass

    meta = {
        "fingerprint": fingerprint,
        "headers": headers,
        "platform": style_profile.get("platform", ""),
        "seller_type": style_profile.get("seller_type", ""),
        "product_count": len(data_model.get("products", [])),
        "recipe_version": recipe.get("version", 1),
        "created_at": datetime.now(timezone.utc).isoformat(),
        "source_job_id": job_id,
    }

    meta_path = cache_path / "meta.json"
    meta_path.write_text(json.dumps(meta, indent=2, ensure_ascii=False))
    logger.info("Saved pipeline cache for user=%s fp=%s from job=%s", username, fingerprint, job_id)


def apply_cache_to_job(username: str, fingerprint: str, job_id: str, mode: str):
    """Copy cached artifacts into a job directory.

    Modes:
        full_reuse   — extraction_script + style_profile + recipe (approved=true)
        adjust_style — extraction_script only
        fresh        — extraction_script only (same as adjust_style)
    """
    cache_path = _cache_dir(username, fingerprint)
    job_path = get_job_path(job_id)

    if not cache_path.exists():
        raise FileNotFoundError(f"Cache not found for {username}/{fingerprint}")

    # Always copy extraction script if available
    es_src = cache_path / "extraction_script.json"
    if es_src.exists():
        shutil.copy2(es_src, job_path / "extraction_script.json")

    if mode == "full_reuse":
        # Copy style profile
        sp_src = cache_path / "style_profile.json"
        if sp_src.exists():
            shutil.copy2(sp_src, job_path / "style_profile.json")

        # Copy recipe and mark as approved
        rp_src = cache_path / "recipe.json"
        if rp_src.exists():
            recipe = json.loads(rp_src.read_text())
            recipe["approved"] = True
            (job_path / "recipe.json").write_text(
                json.dumps(recipe, indent=2, ensure_ascii=False)
            )

    logger.info(
        "Applied cache (mode=%s) for user=%s fp=%s to job=%s",
        mode, username, fingerprint, job_id,
    )
