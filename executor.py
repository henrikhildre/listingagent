"""
Phase 4: Batch Execution.

Applies the approved recipe to every product in the data model.
Products are processed in parallel (5 concurrent) via asyncio.gather.

For each product the module:
1. Fills the prompt template with product data + style profile.
2. Loads the product image (if available).
3. Calls Gemini Flash with structured output (low thinking).
4. Validates the result locally via the recipe's validation code.
5. On validation failure, retries once with higher thinking and the
   validation issues fed back as guidance.
6. Saves the individual listing JSON.
7. Streams progress to connected WebSocket clients.

After the full batch:
- Generates summary.csv and copy-paste text
- Generates batch report (report.json)
- Creates downloadable ZIP via file_utils
"""

import asyncio
import csv
import json
import logging
import time
from datetime import datetime, timezone
from pathlib import Path

from file_utils import get_job_path, load_image_as_bytes, create_output_zip
from gemini_client import (
    generate_structured,
    QuotaExhaustedError,
    BATCH_MODEL,
)
from recipe import (
    fill_template,
    run_validation,
    soften_word_count_issues,
    load_data_model,
    load_style_profile,
    load_recipe,
    DEFAULT_OUTPUT_SCHEMA,
    _parse_word_count_range,
    _fix_word_count,
)

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# WebSocket helpers
# ---------------------------------------------------------------------------


async def _send_ws_message(websocket_connections: set, message: dict):
    """
    Broadcast a JSON message to all connected WebSocket clients.

    Disconnected / broken connections are silently removed from the set.
    """
    if not websocket_connections:
        return

    payload = json.dumps(message, default=str)
    dead = set()

    for ws in websocket_connections:
        try:
            await ws.send_text(payload)
        except Exception:
            # Client disconnected -- mark for removal
            dead.add(ws)

    # Clean up dead connections
    websocket_connections -= dead


async def _send_progress(
    websocket_connections: set,
    product_id: str,
    completed: int,
    total: int,
    score: int | None = None,
    title: str | None = None,
    status: str = "ok",
):
    """Send a typed progress message over the WebSocket."""
    await _send_ws_message(
        websocket_connections,
        {
            "type": "progress",
            "product_id": product_id,
            "completed": completed,
            "total": total,
            "score": score,
            "title": title,
            "status": status,
        },
    )


# ---------------------------------------------------------------------------
# Single-product processing
# ---------------------------------------------------------------------------


async def _process_product(
    job_id: str,
    product: dict,
    recipe: dict,
    style_profile: dict,
    output_schema: dict,
    websocket_connections: set | None = None,
    total: int = 0,
) -> dict:
    """
    Generate a listing for a single product.

    First attempt uses Flash with low thinking (structured output).
    If validation fails, retries once with higher thinking and the
    validation issues included as feedback.

    Returns a result dict with keys:
        product_id, sku, listing, validation, image_filename, retried, failed
    """
    product_id = product.get("id", "unknown")
    sku = product.get("sku")
    image_files = product.get("image_files", [])
    image_filename = image_files[0] if image_files else None

    # Fill prompt
    filled_prompt = fill_template(recipe["prompt_template"], product, style_profile)

    # Load images
    image_parts = _load_product_images(job_id, image_files)

    # --- Attempt 1: Flash, low thinking, structured output ----------------
    try:
        listing = await generate_structured(
            prompt=filled_prompt,
            image_parts=image_parts if image_parts else None,
            schema=output_schema,
            model=BATCH_MODEL,
            thinking_level="low",
        )
    except QuotaExhaustedError:
        raise  # let batch handler deal with quota exhaustion
    except Exception as e:
        logger.error("Gemini call failed for product %s: %s", product_id, e)
        return _failed_result(product_id, sku, image_filename, str(e))

    # Quick word count fix-up before validation
    word_range = _parse_word_count_range(style_profile)
    if word_range and style_profile.get("description_word_count_strict", False):
        wc = len(listing.get("description", "").split())
        wc_min, wc_max = word_range
        if wc < wc_min or wc > wc_max:
            logger.info(
                "Product %s description is %d words (target %d-%d), requesting fix-up",
                product_id, wc, wc_min, wc_max,
            )
            try:
                listing = await _fix_word_count(
                    listing, wc, wc_min, wc_max, output_schema,
                    image_parts=image_parts if image_parts else None,
                )
            except Exception as e:
                logger.warning("Word count fix-up failed for %s: %s", product_id, e)

    # Validate (structural checks only — content quality was proven during recipe testing)
    validation = run_validation(listing, style_profile, "")
    validation = soften_word_count_issues(validation, style_profile)

    if validation.get("passed", False):
        return {
            "product_id": product_id,
            "sku": sku,
            "listing": listing,
            "validation": validation,
            "image_filename": image_filename,
            "retried": False,
            "failed": False,
        }

    # --- Attempt 2: retry with structured output + issue feedback ----------
    logger.info(
        "Product %s failed validation (score=%d), retrying with issue feedback",
        product_id,
        validation.get("score", 0),
    )

    issues = validation.get("issues", [])

    # Notify frontend that this product is being retried
    if websocket_connections is not None:
        first_title = (listing or {}).get("title")
        await _send_ws_message(
            websocket_connections,
            {
                "type": "progress",
                "product_id": product_id,
                "completed": None,
                "total": total,
                "score": None,
                "title": first_title,
                "status": "retrying",
                "issues": issues,
            },
        )

    issues_feedback = "\n".join(f"- {issue}" for issue in issues)
    retry_prompt = (
        f"{filled_prompt}\n\n"
        f"## IMPORTANT — Previous attempt had these issues, please fix them:\n"
        f"{issues_feedback}\n\n"
        f"Make sure to address every issue listed above."
    )

    try:
        listing = await generate_structured(
            prompt=retry_prompt,
            image_parts=image_parts if image_parts else None,
            schema=output_schema,
            model=BATCH_MODEL,
            thinking_level="low",
        )
    except QuotaExhaustedError:
        raise  # let batch handler deal with quota exhaustion
    except Exception as e:
        logger.error("Retry failed for product %s: %s", product_id, e)
        # Return the first attempt's result as a failure
        return {
            "product_id": product_id,
            "sku": sku,
            "listing": listing,  # first attempt listing
            "validation": validation,
            "image_filename": image_filename,
            "retried": True,
            "failed": True,
            "error": str(e),
        }

    # Validate retry result (structural checks only)
    retry_validation = run_validation(listing, style_profile, "")
    retry_validation = soften_word_count_issues(retry_validation, style_profile)

    return {
        "product_id": product_id,
        "sku": sku,
        "listing": listing,
        "validation": retry_validation,
        "image_filename": image_filename,
        "retried": True,
        "failed": not retry_validation.get("passed", False),
    }


def _load_product_images(
    job_id: str, image_files: list[str], max_images: int = 2
) -> list[tuple[bytes, str]]:
    """Load up to max_images for a product, searching images/ and uploads/."""
    job_path = get_job_path(job_id)
    parts: list[tuple[bytes, str]] = []

    for img_filename in image_files[:max_images]:
        for subdir in ("images", "uploads"):
            img_path = job_path / subdir / img_filename
            if img_path.exists():
                try:
                    img_bytes, mime_type = load_image_as_bytes(img_path)
                    parts.append((img_bytes, mime_type))
                except Exception as e:
                    logger.warning("Failed to load image %s: %s", img_path, e)
                break

    return parts


def _failed_result(
    product_id: str,
    sku: str | None,
    image_filename: str | None,
    error: str,
) -> dict:
    """Build a result dict for a product that failed all attempts."""
    return {
        "product_id": product_id,
        "sku": sku,
        "listing": None,
        "validation": {"passed": False, "score": 0, "issues": [error]},
        "image_filename": image_filename,
        "retried": False,
        "failed": True,
        "error": error,
    }


# ---------------------------------------------------------------------------
# Batch execution
# ---------------------------------------------------------------------------


async def execute_batch(job_id: str, websocket_connections: set) -> dict:
    """
    Execute the approved recipe against every product.

    Streams progress via WebSocket and returns a batch report dict.
    """
    # Load artifacts
    recipe = load_recipe(job_id)
    data_model = load_data_model(job_id)
    style_profile = load_style_profile(job_id)

    if not recipe.get("approved"):
        logger.warning("Recipe for job %s is not approved, executing anyway", job_id)

    products = data_model.get("products", [])
    total = len(products)
    output_schema = recipe.get("output_schema", DEFAULT_OUTPUT_SCHEMA)

    logger.info("Starting batch execution for job %s: %d products", job_id, total)

    # Notify clients that execution has started
    await _send_ws_message(
        websocket_connections,
        {
            "type": "batch_start",
            "job_id": job_id,
            "total": total,
        },
    )

    job_path = get_job_path(job_id)
    listings_dir = job_path / "output" / "listings"
    listings_dir.mkdir(parents=True, exist_ok=True)

    start_time = time.monotonic()

    # --- Parallel execution with concurrency limit ---
    CONCURRENCY = 5
    completed_count = 0
    quota_exhausted = False
    semaphore = asyncio.Semaphore(CONCURRENCY)

    async def _process_one(idx: int, product: dict) -> dict:
        nonlocal completed_count, quota_exhausted
        product_id = product.get("id", f"product_{idx}")
        sku = product.get("sku")
        image_files = product.get("image_files", [])
        image_filename = image_files[0] if image_files else None

        if quota_exhausted:
            return _failed_result(product_id, sku, image_filename, "API quota exhausted")

        async with semaphore:
            if quota_exhausted:
                return _failed_result(
                    product_id, sku, image_filename, "API quota exhausted"
                )
            try:
                result = await _process_product(
                    job_id, product, recipe, style_profile, output_schema,
                    websocket_connections=websocket_connections, total=total,
                )
            except QuotaExhaustedError as e:
                logger.error(
                    "Quota exhausted at product %d/%d: %s", idx + 1, total, e
                )
                quota_exhausted = True
                await _send_ws_message(
                    websocket_connections,
                    {
                        "type": "error",
                        "message": "Gemini API quota exhausted. Batch stopped early.",
                    },
                )
                return _failed_result(
                    product_id, sku, image_filename, "API quota exhausted"
                )

        # Save individual listing JSON (even partial / failed ones)
        listing_path = listings_dir / f"{product_id}.json"
        _save_listing(listing_path, result)

        # Stream progress
        completed_count += 1
        score = result.get("validation", {}).get("score")
        title = (result.get("listing") or {}).get("title")
        status = "failed" if result.get("failed") else "ok"

        await _send_progress(
            websocket_connections,
            product_id=product_id,
            completed=completed_count,
            total=total,
            score=score,
            title=title,
            status=status,
        )

        return result

    results = list(await asyncio.gather(
        *[_process_one(idx, product) for idx, product in enumerate(products)]
    ))

    elapsed = time.monotonic() - start_time

    # Post-processing: CSV, report, platform exports, ZIP
    await generate_summary_csv(job_id, results)
    report = generate_batch_report(job_id, results, elapsed_seconds=elapsed)

    # Generate export files
    try:
        generate_copy_paste_text(job_id)
    except Exception as e:
        logger.warning("Export generation failed (non-fatal): %s", e)

    # Package into ZIP
    create_output_zip(job_id)

    # Notify completion
    await _send_ws_message(
        websocket_connections,
        {
            "type": "batch_complete",
            "job_id": job_id,
            "report": report,
        },
    )

    logger.info(
        "Batch execution complete for job %s: %d/%d succeeded in %.1fs",
        job_id,
        report["succeeded"],
        report["total"],
        elapsed,
    )

    return report


def _save_listing(path: Path, result: dict):
    """Persist a single listing result to disk."""
    output = {
        "product_id": result["product_id"],
        "sku": result.get("sku"),
        "listing": result.get("listing"),
        "validation": result.get("validation"),
        "image_filename": result.get("image_filename"),
        "retried": result.get("retried", False),
        "failed": result.get("failed", False),
    }
    if result.get("error"):
        output["error"] = result["error"]

    with open(path, "w", encoding="utf-8") as f:
        json.dump(output, f, indent=2, default=str)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _normalize_specifics(raw) -> dict:
    """Normalize item_specifics to a dict regardless of LLM output format.

    Handles: dict (normal), list of {key, value} dicts, or anything else.
    """
    if isinstance(raw, dict):
        return raw
    if isinstance(raw, list):
        out = {}
        for item in raw:
            if isinstance(item, dict) and "key" in item and "value" in item:
                out[item["key"]] = item["value"]
        return out
    return {}


# ---------------------------------------------------------------------------
# Summary CSV
# ---------------------------------------------------------------------------


async def generate_summary_csv(job_id: str, results: list[dict]) -> Path:
    """
    Write a summary CSV of all generated listings.

    Columns: product_id, sku, title, description, tags, suggested_price,
             confidence, validation_score, image_filename, plus new fields.

    Returns the path to the CSV file.
    """
    job_path = get_job_path(job_id)
    csv_path = job_path / "output" / "summary.csv"

    fieldnames = [
        "product_id",
        "sku",
        "title",
        "description",
        "tags",
        "suggested_price",
        "confidence",
        "validation_score",
        "image_filename",
        "social_caption",
        "hashtags",
        "item_specifics",
        "condition_description",
    ]

    def _write_csv():
        with open(csv_path, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()

            for result in results:
                listing = result.get("listing") or {}
                tags_list = listing.get("tags", [])
                tags_str = (
                    ";".join(tags_list) if isinstance(tags_list, list) else str(tags_list)
                )
                hashtags_list = listing.get("hashtags", [])
                hashtags_str = (
                    ";".join(hashtags_list)
                    if isinstance(hashtags_list, list)
                    else str(hashtags_list)
                )
                specifics = _normalize_specifics(listing.get("item_specifics"))
                specifics_str = "; ".join(f"{k}: {v}" for k, v in specifics.items())

                writer.writerow(
                    {
                        "product_id": result.get("product_id", ""),
                        "sku": result.get("sku") or "",
                        "title": listing.get("title", ""),
                        "description": listing.get("description", ""),
                        "tags": tags_str,
                        "suggested_price": listing.get("suggested_price", ""),
                        "confidence": listing.get("confidence", ""),
                        "validation_score": result.get("validation", {}).get("score", ""),
                        "image_filename": result.get("image_filename") or "",
                        "social_caption": listing.get("social_caption", ""),
                        "hashtags": hashtags_str,
                        "item_specifics": specifics_str,
                        "condition_description": listing.get("condition_description", ""),
                    }
                )

    await asyncio.to_thread(_write_csv)
    logger.info("Summary CSV written to %s", csv_path)
    return csv_path


# ---------------------------------------------------------------------------
# Text export + helpers
# ---------------------------------------------------------------------------


def _get_results_from_disk(job_id: str) -> list[dict]:
    """Load all listing result JSONs from disk."""
    job_path = get_job_path(job_id)
    listings_dir = job_path / "output" / "listings"
    results = []
    if not listings_dir.exists():
        return results
    for p in sorted(listings_dir.glob("*.json")):
        try:
            with open(p, "r", encoding="utf-8") as f:
                results.append(json.load(f))
        except Exception as e:
            logger.warning("Failed to load listing %s: %s", p, e)
    return results


def generate_copy_paste_text(job_id: str) -> Path:
    """
    Generate a plain-text file with all listings formatted for quick
    copy-paste into any platform. Each listing is separated by a divider.
    """
    results = _get_results_from_disk(job_id)
    job_path = get_job_path(job_id)
    txt_path = job_path / "output" / "listings_copy_paste.txt"

    lines = []
    for i, result in enumerate(results):
        listing = result.get("listing") or {}
        if not listing:
            continue

        sku = result.get("sku") or result.get("product_id", "")
        lines.append(f"{'='*60}")
        lines.append(f"LISTING {i+1} — {sku}")
        lines.append(f"{'='*60}")
        lines.append("")
        lines.append(f"TITLE: {listing.get('title', '')}")
        lines.append("")
        lines.append("DESCRIPTION:")
        lines.append(listing.get("description", ""))
        lines.append("")
        tags = listing.get("tags", [])
        if tags:
            lines.append(f"TAGS: {', '.join(tags)}")
            lines.append("")
        price = listing.get("suggested_price")
        if price:
            lines.append(f"PRICE: ${price}")
            lines.append("")
        condition = listing.get("condition_description")
        if condition:
            lines.append(f"CONDITION: {condition}")
            lines.append("")
        specifics = _normalize_specifics(listing.get("item_specifics"))
        if specifics:
            lines.append("ITEM SPECIFICS:")
            for k, v in specifics.items():
                lines.append(f"  {k}: {v}")
            lines.append("")
        caption = listing.get("social_caption")
        if caption:
            lines.append("SOCIAL MEDIA CAPTION:")
            lines.append(caption)
            lines.append("")
        hashtags = listing.get("hashtags", [])
        if hashtags:
            lines.append("HASHTAGS:")
            lines.append(" ".join(f"#{h}" for h in hashtags))
            lines.append("")
        lines.append("")

    with open(txt_path, "w", encoding="utf-8") as f:
        f.write("\n".join(lines))

    logger.info("Copy-paste text written to %s", txt_path)
    return txt_path


# ---------------------------------------------------------------------------
# Batch report
# ---------------------------------------------------------------------------


def generate_batch_report(
    job_id: str,
    results: list[dict],
    elapsed_seconds: float = 0.0,
) -> dict:
    """
    Compute aggregate stats and persist report.json.

    Returns a report dict with: total, succeeded, failed, retried,
    avg_score, elapsed_seconds, completed_at.
    """
    total = len(results)
    succeeded = sum(1 for r in results if not r.get("failed", False))
    failed = total - succeeded
    retried = sum(1 for r in results if r.get("retried", False))

    scores = [
        r.get("validation", {}).get("score", 0)
        for r in results
        if r.get("listing") is not None
    ]
    avg_score = round(sum(scores) / len(scores), 1) if scores else 0.0

    report = {
        "job_id": job_id,
        "total": total,
        "succeeded": succeeded,
        "failed": failed,
        "retried": retried,
        "avg_score": avg_score,
        "elapsed_seconds": round(elapsed_seconds, 1),
        "completed_at": datetime.now(timezone.utc).isoformat(),
    }

    # Also save to disk
    report_path = get_job_path(job_id) / "output" / "report.json"
    with open(report_path, "w", encoding="utf-8") as f:
        json.dump(report, f, indent=2, default=str)

    logger.info(
        "Batch report: %d total, %d succeeded, %d failed, avg_score=%.1f",
        total,
        succeeded,
        failed,
        avg_score,
    )

    return report
