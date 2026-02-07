"""
Phase 1: Data Understanding — explores uploaded files and builds a data model
WITH the user.

Supports three upload scenarios:
  1. Just images (each image = one product)
  2. Excel/CSV + images (most common real scenario)
  3. Images with descriptive filenames (extract info from names)

The build_data_model step uses a two-stage approach:
  A. LLM develops an extraction script via code_execution on a sample
  B. That script runs server-side against the full dataset, with
     validation and iterative fixing if errors occur.

Key outputs:
  - File categorization summary
  - LLM-driven data analysis (with code_execution for spreadsheets)
  - data_model.json saved to the job directory
"""

import ast
import hashlib
import json
import logging
import re

from file_utils import (
    categorize_files,
    get_job_path,
    load_image_as_bytes,
    read_full_csv,
    read_spreadsheet_preview,
    read_spreadsheet_sample,
)
from gemini_client import (
    generate_code_execution_with_parts,
    generate_with_code_execution,
    generate_with_text,
)

logger = logging.getLogger(__name__)

# Maximum number of image thumbnails to send to Gemini during exploration
MAX_PREVIEW_IMAGES = 4


async def categorize_uploads(job_id: str) -> dict:
    """Categorize uploaded files and read spreadsheet previews.

    Scans the job's uploads directory and groups files by type. For any
    spreadsheets found, reads headers + first 5 rows as a preview so
    we can send structured context to the LLM.

    Returns:
        {
            "images": ["photo1.jpg", ...],
            "spreadsheets": ["products.xlsx", ...],
            "documents": ["notes.pdf", ...],
            "other": [...],
            "spreadsheet_previews": {
                "products.xlsx": {"headers": [...], "rows": [...], "total_rows": 47}
            },
            "summary": "Found 52 images, 1 spreadsheet (47 rows), 0 documents."
        }
    """
    categories = categorize_files(job_id)
    uploads_dir = get_job_path(job_id) / "uploads"

    # Read spreadsheet previews
    spreadsheet_previews = {}
    for filename in categories["spreadsheets"]:
        filepath = uploads_dir / filename
        try:
            preview = read_spreadsheet_preview(filepath, max_rows=5)
            spreadsheet_previews[filename] = preview
        except Exception as e:
            logger.warning("Failed to read spreadsheet %s: %s", filename, e)
            spreadsheet_previews[filename] = {"error": str(e)}

    # Build a human-readable summary
    parts = []
    n_images = len(categories["images"])
    n_sheets = len(categories["spreadsheets"])
    n_docs = len(categories["documents"])
    n_other = len(categories["other"])

    if n_images:
        parts.append(f"{n_images} image{'s' if n_images != 1 else ''}")
    if n_sheets:
        row_info = ""
        for fname, preview in spreadsheet_previews.items():
            if "total_rows" in preview:
                row_info = f" ({preview['total_rows']} rows)"
                break
        parts.append(f"{n_sheets} spreadsheet{'s' if n_sheets != 1 else ''}{row_info}")
    if n_docs:
        parts.append(f"{n_docs} document{'s' if n_docs != 1 else ''}")
    if n_other:
        parts.append(f"{n_other} other file{'s' if n_other != 1 else ''}")

    summary = f"Found {', '.join(parts)}." if parts else "No files found."

    return {
        **categories,
        "spreadsheet_previews": spreadsheet_previews,
        "summary": summary,
    }


async def explore_data(
    job_id: str,
    file_summary: dict,
    conversation_history: list[dict] | None = None,
) -> str:
    """Send uploaded data to Gemini for analysis using code_execution.

    Builds a rich prompt with spreadsheet previews, image filenames, and
    up to MAX_PREVIEW_IMAGES thumbnails. The LLM uses code_execution to
    parse headers, detect data types, and try filename-to-SKU matching.

    Args:
        job_id: The job identifier.
        file_summary: Output from categorize_uploads().
        conversation_history: Optional prior messages for follow-up analysis.

    Returns:
        The LLM's analysis text (proposed data model + questions for user).
    """
    uploads_dir = get_job_path(job_id) / "uploads"

    # -- Build the prompt ------------------------------------------------
    prompt_sections = [
        "You are a data analyst helping a marketplace seller understand their "
        "product data. Analyze the uploaded files and propose how to structure "
        "them into a product catalog.\n",
        f"## Upload Summary\n{file_summary['summary']}\n",
    ]

    # Spreadsheet details
    for filename, preview in file_summary.get("spreadsheet_previews", {}).items():
        if "error" in preview:
            prompt_sections.append(
                f"## Spreadsheet: {filename}\nError reading file: {preview['error']}\n"
            )
            continue

        headers_str = " | ".join(preview["headers"])
        rows_str = "\n".join(" | ".join(row) for row in preview["rows"])
        prompt_sections.append(
            f"## Spreadsheet: {filename}\n"
            f"Total rows: {preview['total_rows']}\n"
            f"Headers: {headers_str}\n"
            f"Sample data:\n{rows_str}\n"
        )

    # Image filenames
    images = file_summary.get("images", [])
    if images:
        # Show all filenames so the LLM can detect naming patterns
        image_list = "\n".join(f"  - {name}" for name in sorted(images))
        prompt_sections.append(f"## Image Files ({len(images)} total)\n{image_list}\n")

    # Prior conversation context
    if conversation_history:
        history_text = "\n".join(
            f"{'User' if m.get('role') == 'user' else 'Assistant'}: {m.get('content', '')}"
            for m in conversation_history
        )
        prompt_sections.append(f"## Previous Conversation\n{history_text}\n")

    prompt_sections.append(
        "## Your Task\n"
        "1. Use code_execution to analyze the data structure — parse headers, "
        "detect data types, find patterns in filenames.\n"
        "2. Figure out how images map to spreadsheet rows (if a spreadsheet exists). "
        "Look for SKU patterns, name matches, or numbering conventions.\n"
        "3. Propose a data model: what fields each product has, how images are "
        "linked, and any ambiguities.\n\n"
        "If there is NO spreadsheet, treat each image as a product and extract "
        "any info you can from the filenames (e.g., 'blue-wool-scarf-25cm.jpg' "
        "-> name='Blue Wool Scarf', size='25cm').\n\n"
        "Be concise and conversational. Present your findings clearly.\n\n"
        "IMPORTANT: End your response with a clear conclusion. Show a brief summary "
        "of what you found (e.g., '15 products, 42 images matched, 3 unmatched') "
        'and then say: "Review the mapping above — if everything looks correct, '
        'click **Confirm Data Mapping**. Otherwise, let me know what to adjust."\n'
        "Do NOT ask open-ended questions like 'How would you like to proceed?'"
    )

    prompt = "\n".join(prompt_sections)

    # -- Load sample images for multimodal context -----------------------
    image_parts = []
    sample_images = sorted(images)[:MAX_PREVIEW_IMAGES]
    for img_name in sample_images:
        img_path = uploads_dir / img_name
        if img_path.exists():
            try:
                img_bytes, mime_type = load_image_as_bytes(img_path)
                image_parts.append((img_bytes, mime_type))
            except Exception as e:
                logger.warning("Failed to load image %s: %s", img_name, e)

    # -- Call Gemini with code_execution ---------------------------------
    response_text = await generate_with_code_execution(
        prompt,
        image_parts=image_parts if image_parts else None,
        thinking_level="high",
    )

    return response_text


MAX_EXTRACTION_RETRIES = 3

# Modules the LLM extraction script is allowed to import
_ALLOWED_IMPORTS = {"pandas", "pd", "io", "json", "re", "math"}


async def build_data_model(job_id: str, conversation_history: list[dict]) -> dict:
    """Build data_model.json using a script-based extraction pipeline.

    Instead of asking the LLM to generate all product rows (which it can't
    do reliably for large datasets), this:
      1. Sends a strategic sample of the spreadsheet to the LLM with
         code_execution enabled so it can develop a pandas script.
      2. Extracts the script and runs it server-side against the full dataset.
      3. Validates results and iterates with the LLM on errors.

    Falls back to image-only mode if there is no spreadsheet.
    """
    job_path = get_job_path(job_id)
    file_summary = await categorize_uploads(job_id)
    uploads_dir = job_path / "uploads"

    spreadsheets = file_summary.get("spreadsheets", [])
    all_images = sorted(file_summary.get("images", []))

    if not spreadsheets:
        data_model = _build_image_only_data_model(all_images)
    else:
        sheet_path = uploads_dir / spreadsheets[0]
        data_model = await _build_spreadsheet_data_model(
            job_path, sheet_path, spreadsheets[0], all_images, conversation_history
        )

    # Compute quality report and field stats
    data_model["quality_report"] = _build_quality_report(data_model)
    data_model["field_stats"] = _build_field_stats(data_model)

    # Save to job directory
    output_path = job_path / "data_model.json"
    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(data_model, f, indent=2, ensure_ascii=False)

    logger.info(
        "Saved data_model.json for job %s (%d products)",
        job_id,
        len(data_model.get("products", [])),
    )
    return data_model


# ---------------------------------------------------------------------------
# Spreadsheet extraction via LLM-generated script
# ---------------------------------------------------------------------------


async def _build_spreadsheet_data_model(
    job_path,
    sheet_path,
    sheet_filename: str,
    all_images: list[str],
    conversation_history: list[dict],
) -> dict:
    """Full pipeline: sample → LLM script → server-side exec → validate → iterate.

    If a previously saved extraction script matches the column fingerprint of
    this spreadsheet, it is reused without calling the LLM.
    """
    sample = read_spreadsheet_sample(sheet_path)
    full_csv = read_full_csv(sheet_path)
    col_fingerprint = _column_fingerprint(sample["headers"])

    # Try reusing a saved script with matching columns
    script = _load_saved_script(job_path, col_fingerprint)
    if script:
        logger.info("Reusing saved extraction script (fingerprint %s)", col_fingerprint)
    else:
        # Step 1: LLM develops script on sample
        script, llm_text = await _develop_extraction_script(
            sample, all_images, conversation_history
        )
        if not script:
            raise ValueError("LLM did not produce an extraction script")

    # Step 2: Run server-side, validate, iterate on errors
    products = await _run_and_validate_script(
        script, full_csv, sample["total_rows"], all_images,
        sample, conversation_history,
    )

    # Save the working script for reuse
    _save_extraction_script(job_path, script, col_fingerprint, sample["headers"])

    return _assemble_data_model(
        products, all_images, sheet_filename,
        sample["headers"], sample["total_rows"],
    )


async def _develop_extraction_script(
    sample: dict,
    all_images: list[str],
    conversation_history: list[dict],
) -> tuple[str | None, str]:
    """Ask the LLM to write a pandas extraction script using code_execution.

    The sample CSV is attached as an inline file part so the LLM can test
    its code against real data inside the sandbox.

    Returns (script_code, llm_text_response).
    """
    transcript = "\n".join(
        f"{'User' if m.get('role') == 'user' else 'Assistant'}: {m.get('content', '')}"
        for m in conversation_history[-6:]
    )

    image_list = "\n".join(f"  - {name}" for name in all_images[:50])
    if len(all_images) > 50:
        image_list += f"\n  ... and {len(all_images) - 50} more"

    prompt = f"""\
You are building a data extraction script for a product catalog.

## Sample Data (CSV)
The attached CSV contains a representative sample of the full dataset \
({sample['total_rows']} total rows). Use it to develop and test your script.

## Image Files ({len(all_images)} total)
{image_list}

## Context from User Conversation
{transcript}

## Your Task
Write a Python script using pandas that:

1. Reads a CSV from a string variable called `csv_data` using:
   `df = pd.read_csv(io.StringIO(csv_data))`

2. Cleans and normalizes the data:
   - Strip whitespace from all string columns
   - Handle missing/null values (replace NaN with None, not the string "nan")
   - Parse prices to float if a price column exists (handle currency symbols etc.)
   - Drop fully empty rows

3. Discovers all meaningful product fields from the columns.

4. Tries to match image files to products. Look for SKU, product name, or ID
   patterns in the image filenames. The list of filenames is in a variable
   called `image_filenames`.

5. Assigns the final result to a variable called `result_json` as a JSON
   string with this structure:
   ```
   {{
     "fields_discovered": ["field1", "field2", ...],
     "products": [
       {{
         "id": "product_001",
         "image_files": ["matched_image.jpg"],
         "source": "spreadsheet_row_1",
         ... all discovered fields as key-value pairs ...
       }}
     ],
     "image_matching_strategy": "description of how images were matched"
   }}
   ```

IMPORTANT RULES:
- The script MUST work on both the sample now AND the full dataset later.
- Use `csv_data` as input (a string). Use `image_filenames` (a list of strings).
- Assign the final JSON string to `result_json`.
- Do NOT hardcode row counts or values from the sample.
- Handle edge cases: empty cells, mixed types, encoding oddities.
- Every product must have a unique "id" (e.g., "product_001", "product_002").
- The "source" field should reference the row (e.g., "spreadsheet_row_1").

Test your script on the attached sample. Print a summary at the end showing
the row count and fields found."""

    text_response, script = await generate_code_execution_with_parts(
        prompt,
        csv_data=sample["sample_csv"],
        thinking_level="high",
    )
    return script, text_response


async def _run_and_validate_script(
    script: str,
    full_csv: str,
    expected_rows: int,
    all_images: list[str],
    sample: dict,
    conversation_history: list[dict],
) -> list[dict]:
    """Run extraction script server-side, validate, fix if needed.

    Iterates up to MAX_EXTRACTION_RETRIES times on failure.
    """
    current_script = script

    for attempt in range(MAX_EXTRACTION_RETRIES):
        products, errors = _execute_extraction_script(
            current_script, full_csv, all_images, expected_rows
        )

        if not errors:
            return products

        logger.warning(
            "Extraction attempt %d/%d had %d error(s): %s",
            attempt + 1, MAX_EXTRACTION_RETRIES, len(errors), errors[:3],
        )

        if attempt < MAX_EXTRACTION_RETRIES - 1:
            fixed = await _fix_extraction_script(
                current_script, errors, sample, all_images,
            )
            if fixed:
                current_script = fixed
            else:
                break

    # Return best-effort result even if there are remaining errors
    if products:
        logger.warning("Returning %d products despite errors: %s", len(products), errors)
        return products
    raise ValueError(f"Extraction failed after {MAX_EXTRACTION_RETRIES} attempts: {errors}")


def _execute_extraction_script(
    script: str,
    full_csv: str,
    image_filenames: list[str],
    expected_rows: int,
) -> tuple[list[dict], list[str]]:
    """Execute the LLM-generated script in a restricted sandbox with pandas.

    Returns (products_list, errors_list).
    """
    errors: list[str] = []

    safety_error = _check_extraction_code_safety(script)
    if safety_error:
        return [], [f"Script safety check failed: {safety_error}"]

    import io as io_module
    import json as json_module
    import math as math_module
    import re as re_module
    import pandas as pd_module

    sandbox_globals = {
        "__builtins__": _extraction_safe_builtins(),
        "pd": pd_module,
        "io": io_module,
        "json": json_module,
        "re": re_module,
        "math": math_module,
    }
    sandbox_locals = {
        "csv_data": full_csv,
        "image_filenames": image_filenames,
        "result_json": None,
    }

    try:
        exec(script, sandbox_globals, sandbox_locals)  # noqa: S102
    except Exception as e:
        return [], [f"Script execution error: {type(e).__name__}: {e}"]

    # Extract results
    raw = sandbox_locals.get("result_json")
    if raw is None:
        return [], ["Script did not assign a value to result_json"]

    try:
        result = json.loads(raw) if isinstance(raw, str) else raw
    except (json.JSONDecodeError, TypeError) as e:
        return [], [f"result_json is not valid JSON: {e}"]

    products = result.get("products", [])

    if not products:
        errors.append("Script produced 0 products")
    elif len(products) < expected_rows * 0.8:
        errors.append(
            f"Row count mismatch: got {len(products)} products "
            f"but spreadsheet has {expected_rows} rows"
        )

    # Detect mostly-empty products
    null_heavy = 0
    for p in products:
        non_null = sum(
            1 for k, v in p.items()
            if v is not None and v != "" and v != "None" and k not in ("id", "source")
        )
        if non_null <= 1:
            null_heavy += 1
    if products and null_heavy > len(products) * 0.5:
        errors.append(f"{null_heavy}/{len(products)} products have almost no data")

    return products, errors


async def _fix_extraction_script(
    script: str,
    errors: list[str],
    sample: dict,
    all_images: list[str],
) -> str | None:
    """Send the broken script + errors back to the LLM for a fix."""
    error_text = "\n".join(f"- {e}" for e in errors)

    prompt = f"""\
The following extraction script had errors when run against the full dataset.

## Script
```python
{script}
```

## Errors
{error_text}

## Sample Data Headers
{', '.join(sample['headers'])}

## Total Expected Rows
{sample['total_rows']}

Fix the script to address these errors. Keep the same interface:
- Read from `csv_data` (string) and `image_filenames` (list)
- Assign JSON string to `result_json`

Test on the attached sample data, then print a summary."""

    text_response, fixed_script = await generate_code_execution_with_parts(
        prompt,
        csv_data=sample["sample_csv"],
        thinking_level="high",
    )
    return fixed_script


# ---------------------------------------------------------------------------
# Sandbox safety
# ---------------------------------------------------------------------------


def _check_extraction_code_safety(code: str) -> str | None:
    """AST safety check for extraction scripts.

    Allows imports of pandas, io, json, re, math but blocks everything
    else that could be dangerous.
    """
    try:
        tree = ast.parse(code)
    except SyntaxError as e:
        return f"Syntax error: {e}"

    for node in ast.walk(tree):
        if isinstance(node, ast.Attribute) and node.attr.startswith("__"):
            return f"Blocked: access to dunder attribute '{node.attr}'"
        if isinstance(node, ast.Import):
            for alias in node.names:
                if alias.name not in _ALLOWED_IMPORTS:
                    return f"Blocked: import of '{alias.name}'"
        if isinstance(node, ast.ImportFrom):
            if node.module and node.module.split(".")[0] not in _ALLOWED_IMPORTS:
                return f"Blocked: import from '{node.module}'"
        if isinstance(node, ast.Call) and isinstance(node.func, ast.Name):
            if node.func.id in ("exec", "eval", "compile", "__import__", "open"):
                return f"Blocked: call to '{node.func.id}'"

    return None


def _extraction_safe_builtins() -> dict:
    """Builtins available inside the extraction sandbox."""
    return {
        "len": len, "str": str, "int": int, "float": float,
        "bool": bool, "list": list, "dict": dict, "set": set,
        "tuple": tuple, "range": range, "enumerate": enumerate,
        "zip": zip, "map": map, "filter": filter, "sorted": sorted,
        "min": min, "max": max, "sum": sum, "abs": abs, "round": round,
        "any": any, "all": all, "isinstance": isinstance, "type": type,
        "hasattr": hasattr, "getattr": getattr,
        "print": print,
        "True": True, "False": False, "None": None,
        "ValueError": ValueError, "TypeError": TypeError,
        "KeyError": KeyError, "IndexError": IndexError,
        "Exception": Exception,
        "repr": repr, "chr": chr, "ord": ord,
    }


# ---------------------------------------------------------------------------
# Image-only fallback
# ---------------------------------------------------------------------------


def _build_image_only_data_model(all_images: list[str]) -> dict:
    """When there is no spreadsheet, each image becomes a product."""
    products = []
    for i, img in enumerate(all_images, 1):
        products.append({
            "id": f"product_{i:03d}",
            "image_files": [img],
            "source": "image_only",
        })

    return {
        "sources": {
            "images": {
                "total": len(all_images),
                "matched": len(all_images),
                "unmatched": [],
            }
        },
        "fields_discovered": [],
        "products": products,
        "unmatched_images": [],
        "matching_strategy": "Each image treated as a separate product",
    }


# ---------------------------------------------------------------------------
# Assembly helpers
# ---------------------------------------------------------------------------


def _assemble_data_model(
    products: list[dict],
    all_images: list[str],
    sheet_filename: str,
    headers: list[str],
    total_rows: int,
) -> dict:
    """Build the final data_model.json structure from extraction results."""
    fields_discovered = set()
    for p in products:
        fields_discovered.update(p.keys())
    fields_discovered -= {"id", "source", "image_files"}

    matched_images = set()
    for p in products:
        for img in p.get("image_files", []):
            matched_images.add(img)
    unmatched = [img for img in all_images if img not in matched_images]

    return {
        "sources": {
            "spreadsheet": {
                "filename": sheet_filename,
                "columns": {h: "" for h in headers},
                "row_count": total_rows,
            },
            "images": {
                "total": len(all_images),
                "matched": len(matched_images),
                "unmatched": unmatched,
            },
        },
        "fields_discovered": sorted(fields_discovered),
        "products": products,
        "unmatched_images": [
            {"filename": f, "status": "unmatched"} for f in unmatched
        ],
        "matching_strategy": "Script-based extraction with image filename matching",
    }


# ---------------------------------------------------------------------------
# Quality report
# ---------------------------------------------------------------------------


def _build_quality_report(data_model: dict) -> dict:
    """Analyze extracted products and return a data quality summary.

    The report gives the user a clear picture of what was extracted and
    what might need attention before proceeding.
    """
    products = data_model.get("products", [])
    fields = data_model.get("fields_discovered", [])
    unmatched = data_model.get("unmatched_images", [])

    total = len(products)
    if total == 0:
        return {"total_products": 0, "warnings": ["No products extracted"]}

    # Per-field completeness
    field_completeness = {}
    for field in fields:
        filled = sum(
            1 for p in products
            if p.get(field) is not None and str(p.get(field)).strip() not in ("", "N/A", "None")
        )
        field_completeness[field] = {
            "filled": filled,
            "total": total,
            "pct": round(100 * filled / total),
        }

    # Image coverage
    with_images = sum(1 for p in products if p.get("image_files"))
    without_images = total - with_images

    # Warnings
    warnings = []
    for field, info in field_completeness.items():
        if info["pct"] < 50:
            warnings.append(f'"{field}" is only {info["pct"]}% filled ({info["filled"]}/{total})')
    if without_images > 0:
        warnings.append(f"{without_images} product(s) have no matched images")
    if unmatched:
        warnings.append(f"{len(unmatched)} image(s) could not be matched to any product")

    return {
        "total_products": total,
        "fields_discovered": fields,
        "field_completeness": field_completeness,
        "images_matched": with_images,
        "images_unmatched": len(unmatched),
        "products_without_images": without_images,
        "warnings": warnings,
    }


# ---------------------------------------------------------------------------
# Field stats for recipe drafting
# ---------------------------------------------------------------------------


def _build_field_stats(data_model: dict) -> dict:
    """Compute per-field statistics to help the recipe LLM write better prompts.

    For each discovered field, reports the data type, sample values,
    and (for numeric fields) the value range.
    """
    products = data_model.get("products", [])
    fields = data_model.get("fields_discovered", [])

    if not products:
        return {}

    stats = {}
    for field in fields:
        values = [
            p.get(field) for p in products
            if p.get(field) is not None and str(p.get(field)).strip() not in ("", "N/A", "None")
        ]
        if not values:
            stats[field] = {"type": "empty", "filled": 0}
            continue

        # Try to detect numeric fields
        numeric_vals = []
        for v in values:
            try:
                numeric_vals.append(float(v))
            except (ValueError, TypeError):
                break  # not a numeric field

        if len(numeric_vals) == len(values) and numeric_vals:
            stats[field] = {
                "type": "numeric",
                "filled": len(values),
                "min": round(min(numeric_vals), 2),
                "max": round(max(numeric_vals), 2),
                "sample": [str(v) for v in values[:3]],
            }
        else:
            unique = list(dict.fromkeys(str(v) for v in values))  # ordered unique
            stats[field] = {
                "type": "text",
                "filled": len(values),
                "unique_count": len(unique),
                "sample": unique[:5],
            }

    return stats


# ---------------------------------------------------------------------------
# Extraction script persistence
# ---------------------------------------------------------------------------


def _column_fingerprint(headers: list[str]) -> str:
    """Stable hash of column headers so we can match scripts to formats."""
    normalized = "|".join(h.strip().lower() for h in sorted(headers))
    return hashlib.sha256(normalized.encode()).hexdigest()[:16]


def _save_extraction_script(
    job_path, script: str, fingerprint: str, headers: list[str],
):
    """Persist the working extraction script for potential reuse."""
    script_meta = {
        "fingerprint": fingerprint,
        "headers": headers,
        "script": script,
    }
    path = job_path / "extraction_script.json"
    with open(path, "w", encoding="utf-8") as f:
        json.dump(script_meta, f, indent=2, ensure_ascii=False)
    logger.info("Saved extraction script (fingerprint %s)", fingerprint)


def _load_saved_script(job_path, fingerprint: str) -> str | None:
    """Load a saved extraction script if it matches the column fingerprint."""
    path = job_path / "extraction_script.json"
    if not path.exists():
        return None
    try:
        with open(path, encoding="utf-8") as f:
            meta = json.load(f)
        if meta.get("fingerprint") == fingerprint:
            return meta.get("script")
    except (json.JSONDecodeError, KeyError):
        pass
    return None


# ---------------------------------------------------------------------------
# JSON parsing (kept for other callers)
# ---------------------------------------------------------------------------


def _parse_json_from_response(text: str) -> dict:
    """Extract JSON from a model response that may contain markdown fences.

    Tries multiple strategies:
      1. Look for ```json ... ``` fenced block
      2. Look for ``` ... ``` fenced block
      3. Try parsing the entire text as JSON
      4. Find the first { ... } block using brace matching

    Args:
        text: Raw response text from the model.

    Returns:
        Parsed dictionary.

    Raises:
        ValueError: If no valid JSON could be extracted.
    """
    if not text or not text.strip():
        raise ValueError("Empty response text — cannot extract JSON")

    # Strategy 1: ```json ... ```
    match = re.search(r"```json\s*\n?(.*?)\n?\s*```", text, re.DOTALL)
    if match:
        try:
            return json.loads(match.group(1))
        except json.JSONDecodeError:
            pass

    # Strategy 2: ``` ... ```
    match = re.search(r"```\s*\n?(.*?)\n?\s*```", text, re.DOTALL)
    if match:
        try:
            return json.loads(match.group(1))
        except json.JSONDecodeError:
            pass

    # Strategy 3: Try the whole text
    try:
        return json.loads(text.strip())
    except json.JSONDecodeError:
        pass

    # Strategy 4: Find the outermost { ... } using brace counting
    start = text.find("{")
    if start != -1:
        depth = 0
        for i in range(start, len(text)):
            if text[i] == "{":
                depth += 1
            elif text[i] == "}":
                depth -= 1
                if depth == 0:
                    try:
                        return json.loads(text[start : i + 1])
                    except json.JSONDecodeError:
                        break

    raise ValueError(
        f"Could not extract valid JSON from response. First 200 chars: {text[:200]}"
    )
