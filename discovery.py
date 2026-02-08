"""
Phase 1: Data Understanding — explores uploaded files and builds a data model
WITH the user.

Supports multiple input scenarios:
  1. Excel/CSV + images (most common — code execution extraction)
  2. JSON files (code execution extraction, same pipeline as spreadsheets)
  3. Pasted text (direct LLM extraction for small text, code exec for large)
  4. Just images (vision-based extraction — LLM looks at photos)
  5. Images with descriptive filenames (extract info from names)

The build_data_model step routes to the appropriate extraction strategy:
  - code_execution: for spreadsheets, JSON, and large pasted text
  - direct_llm: for small pasted text (fits in context)
  - vision: for image-only uploads (LLM analyzes photos)

Key outputs:
  - File categorization summary
  - LLM-driven data analysis
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
    get_pasted_text,
    load_image_as_bytes,
    read_full_csv,
    read_full_json,
    read_json_preview,
    read_json_sample,
    read_spreadsheet_preview,
    read_spreadsheet_sample,
)
from gemini_client import (
    generate_code_execution_with_parts,
    generate_with_code_execution,
    generate_with_images,
    generate_with_text,
)

logger = logging.getLogger(__name__)

# Maximum number of image thumbnails to send to Gemini during exploration
MAX_PREVIEW_IMAGES = 4
# More images sent when doing vision-only analysis (no spreadsheet)
MAX_VISION_PREVIEW_IMAGES = 8
# Batch size for vision extraction in build_data_model
VISION_BATCH_SIZE = 5
# Pasted text below this threshold uses direct LLM extraction;
# above it, the text is treated as structured data for code execution.
MAX_PASTE_DIRECT_EXTRACTION = 30_000  # characters


async def categorize_uploads(job_id: str) -> dict:
    """Categorize uploaded files, read previews, and detect pasted text.

    Scans the job's uploads directory and groups files by type. For any
    spreadsheets found, reads headers + first 5 rows as a preview so
    we can send structured context to the LLM.  Also reads JSON file
    previews and checks for pasted text input.

    Returns:
        {
            "images": ["photo1.jpg", ...],
            "spreadsheets": ["products.xlsx", ...],
            "json_files": ["data.json", ...],
            "documents": ["notes.pdf", ...],
            "other": [...],
            "spreadsheet_previews": { ... },
            "json_previews": { ... },
            "pasted_text": "..." or None,
            "summary": "Found 52 images, 1 spreadsheet (47 rows), ..."
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

    # Read JSON file previews
    json_previews = {}
    for filename in categories.get("json_files", []):
        filepath = uploads_dir / filename
        try:
            preview = read_json_preview(filepath, max_items=5)
            json_previews[filename] = preview
        except Exception as e:
            logger.warning("Failed to read JSON file %s: %s", filename, e)
            json_previews[filename] = {"error": str(e)}

    # Check for pasted text
    pasted_text = get_pasted_text(job_id)

    # Build a human-readable summary
    parts = []
    n_images = len(categories["images"])
    n_sheets = len(categories["spreadsheets"])
    n_json = len(categories.get("json_files", []))
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
    if n_json:
        row_info = ""
        for fname, preview in json_previews.items():
            if "total_rows" in preview:
                row_info = f" ({preview['total_rows']} items)"
                break
        parts.append(f"{n_json} JSON file{'s' if n_json != 1 else ''}{row_info}")
    if pasted_text:
        parts.append(f"pasted text ({len(pasted_text):,} characters)")
    if n_docs:
        parts.append(f"{n_docs} document{'s' if n_docs != 1 else ''}")
    if n_other:
        parts.append(f"{n_other} other file{'s' if n_other != 1 else ''}")

    summary = f"Found {', '.join(parts)}." if parts else "No data found."

    return {
        **categories,
        "spreadsheet_previews": spreadsheet_previews,
        "json_previews": json_previews,
        "pasted_text": pasted_text,
        "summary": summary,
    }


async def explore_data(
    job_id: str,
    file_summary: dict,
    conversation_history: list[dict] | None = None,
) -> str:
    """Send uploaded data to Gemini for analysis.

    Adapts the prompt and generation strategy based on what data is available:
    - Spreadsheets/JSON: code_execution for structural analysis
    - Pasted text: direct analysis (with code_execution if structured)
    - Images only: vision-focused multimodal analysis with more thumbnails

    Args:
        job_id: The job identifier.
        file_summary: Output from categorize_uploads().
        conversation_history: Optional prior messages for follow-up analysis.

    Returns:
        The LLM's analysis text (proposed data model + questions for user).
    """
    uploads_dir = get_job_path(job_id) / "uploads"
    has_spreadsheets = bool(file_summary.get("spreadsheets"))
    has_json = bool(file_summary.get("json_files"))
    has_structured_data = has_spreadsheets or has_json
    pasted_text = file_summary.get("pasted_text")
    images = file_summary.get("images", [])
    is_image_only = images and not has_structured_data and not pasted_text

    # -- Build the prompt ------------------------------------------------
    prompt_sections = [
        "You are a data analyst helping a marketplace seller understand their "
        "product data. Analyze what they've provided and propose how to structure "
        "it into a product catalog.\n",
        f"## Input Summary\n{file_summary['summary']}\n",
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

    # JSON file details
    for filename, preview in file_summary.get("json_previews", {}).items():
        if "error" in preview:
            prompt_sections.append(
                f"## JSON File: {filename}\nError reading file: {preview['error']}\n"
            )
            continue

        headers_str = " | ".join(preview.get("headers", []))
        rows_str = "\n".join(" | ".join(row) for row in preview.get("rows", []))
        fmt = preview.get("format", "unknown")
        array_key = preview.get("array_key", "")
        key_note = f" (items under key \"{array_key}\")" if array_key else ""
        prompt_sections.append(
            f"## JSON File: {filename} [{fmt}{key_note}]\n"
            f"Total items: {preview.get('total_rows', '?')}\n"
            f"Fields: {headers_str}\n"
            f"Sample data:\n{rows_str}\n"
        )

    # Pasted text
    if pasted_text:
        text_preview = pasted_text[:5000]
        if len(pasted_text) > 5000:
            text_preview += f"\n\n... (truncated — {len(pasted_text):,} total characters)"
        prompt_sections.append(f"## Pasted Text Input\n{text_preview}\n")

    # Image filenames
    if images:
        image_list = "\n".join(f"  - {name}" for name in sorted(images))
        prompt_sections.append(f"## Image Files ({len(images)} total)\n{image_list}\n")

    # Prior conversation context
    if conversation_history:
        history_text = "\n".join(
            f"{'User' if m.get('role') == 'user' else 'Assistant'}: {m.get('content', '')}"
            for m in conversation_history
        )
        prompt_sections.append(f"## Previous Conversation\n{history_text}\n")

    # -- Task instructions vary by scenario ------------------------------
    if is_image_only:
        prompt_sections.append(
            "## Your Task\n"
            "The user has uploaded product images with NO spreadsheet or text data.\n"
            "1. Look at the attached product photos and describe what kinds of "
            "products these appear to be.\n"
            "2. Propose what attributes you can extract from the photos "
            "(category, color, material, condition, style/era, etc.).\n"
            "3. Check image filenames for patterns that encode product info "
            "(e.g., 'blue-wool-scarf-25cm.jpg').\n"
            "4. Propose a data model with the fields you can extract.\n\n"
            "Be concise and conversational. Present your findings clearly.\n\n"
            "IMPORTANT: End with a summary (e.g., '20 product images found, "
            "categories include lamps, vases, textiles') and say: "
            '"Review the analysis above — if this looks right, click '
            '**Confirm Data Mapping**. Otherwise, let me know what to adjust."\n'
            "Do NOT ask open-ended questions like 'How would you like to proceed?'"
        )
    elif pasted_text and not has_structured_data:
        prompt_sections.append(
            "## Your Task\n"
            "The user has pasted text containing product information.\n"
            "1. Analyze the text to identify individual products or items.\n"
            "2. Detect what fields/attributes are present (name, price, "
            "category, description, condition, etc.).\n"
            "3. If the text looks like structured data (JSON, CSV, "
            "tab-separated), parse the structure.\n"
            "4. Propose a data model: what fields each product has.\n"
            "5. If images were also uploaded, figure out how they map to "
            "the products.\n\n"
            "Be concise and conversational. Present your findings clearly.\n\n"
            "IMPORTANT: End with a summary (e.g., '12 products found with "
            "fields: name, price, condition') and say: "
            '"Review the mapping above — if everything looks correct, click '
            '**Confirm Data Mapping**. Otherwise, let me know what to adjust."\n'
            "Do NOT ask open-ended questions like 'How would you like to proceed?'"
        )
    else:
        # Spreadsheet / JSON / mixed — existing task
        prompt_sections.append(
            "## Your Task\n"
            "1. Use code_execution to analyze the data structure — parse headers, "
            "detect data types, find patterns in filenames.\n"
            "2. Figure out how images map to data rows (if applicable). "
            "Look for SKU patterns, name matches, or numbering conventions.\n"
            "3. Propose a data model: what fields each product has, how images are "
            "linked, and any ambiguities.\n\n"
            "Be concise and conversational. Present your findings clearly.\n\n"
            "IMPORTANT: End your response with a clear conclusion. Show a brief summary "
            "of what you found (e.g., '15 products, 42 images matched, 3 unmatched') "
            'and then say: "Review the mapping above — if everything looks correct, '
            'click **Confirm Data Mapping**. Otherwise, let me know what to adjust."\n'
            "Do NOT ask open-ended questions like 'How would you like to proceed?'"
        )

    prompt = "\n".join(prompt_sections)

    # -- Load sample images for multimodal context -----------------------
    max_images = MAX_VISION_PREVIEW_IMAGES if is_image_only else MAX_PREVIEW_IMAGES
    image_parts = []
    sample_images = sorted(images)[:max_images]
    for img_name in sample_images:
        img_path = uploads_dir / img_name
        if img_path.exists():
            try:
                img_bytes, mime_type = load_image_as_bytes(img_path)
                image_parts.append((img_bytes, mime_type))
            except Exception as e:
                logger.warning("Failed to load image %s: %s", img_name, e)

    # -- Call Gemini — strategy depends on data type ---------------------
    if has_structured_data:
        # Spreadsheet/JSON: code_execution for analysis
        response_text = await generate_with_code_execution(
            prompt,
            image_parts=image_parts if image_parts else None,
            thinking_level="high",
        )
    elif is_image_only:
        # Vision-focused: multimodal generation (no code exec needed)
        if image_parts:
            response_text = await generate_with_images(
                prompt, image_parts, thinking_level="high",
            )
        else:
            response_text = await generate_with_text(prompt, thinking_level="high")
    elif pasted_text:
        # Pasted text: use code_execution if it looks structured, else text-only
        if image_parts:
            response_text = await generate_with_code_execution(
                prompt,
                image_parts=image_parts,
                thinking_level="high",
            )
        else:
            response_text = await generate_with_code_execution(
                prompt, thinking_level="high",
            )
    else:
        response_text = await generate_with_text(prompt, thinking_level="high")

    return response_text


MAX_EXTRACTION_RETRIES = 3

# Modules the LLM extraction script is allowed to import
_ALLOWED_IMPORTS = {"pandas", "pd", "io", "json", "re", "math"}


async def build_data_model(job_id: str, conversation_history: list[dict]) -> dict:
    """Build data_model.json by routing to the best extraction strategy.

    Strategy routing:
      - Spreadsheets (xlsx/csv/tsv): code execution (LLM writes pandas script)
      - JSON files: code execution (LLM writes json parsing script)
      - Pasted text (small): direct LLM extraction
      - Pasted text (large): code execution
      - Images only: vision extraction (LLM analyzes photos)
    """
    job_path = get_job_path(job_id)
    file_summary = await categorize_uploads(job_id)
    uploads_dir = job_path / "uploads"

    spreadsheets = file_summary.get("spreadsheets", [])
    json_files = file_summary.get("json_files", [])
    all_images = sorted(file_summary.get("images", []))
    pasted_text = file_summary.get("pasted_text")

    # Route to extraction strategy
    if spreadsheets:
        # Spreadsheet path — existing code execution pipeline
        sheet_path = uploads_dir / spreadsheets[0]
        data_model = await _build_spreadsheet_data_model(
            job_path, sheet_path, spreadsheets[0], all_images, conversation_history
        )
    elif json_files:
        # JSON path — code execution with json parsing
        json_path = uploads_dir / json_files[0]
        data_model = await _build_json_data_model(
            job_path, json_path, json_files[0], all_images, conversation_history
        )
    elif pasted_text:
        if len(pasted_text) > MAX_PASTE_DIRECT_EXTRACTION:
            # Large paste — treat as structured data, code execution
            data_model = await _build_large_paste_data_model(
                job_path, pasted_text, all_images, conversation_history
            )
        else:
            # Small paste — direct LLM extraction
            data_model = await _build_paste_data_model(
                pasted_text, all_images, conversation_history
            )
    elif all_images:
        # Image-only — vision extraction
        data_model = await _build_vision_data_model(
            job_id, all_images, conversation_history
        )
    else:
        raise ValueError("No data found to process — upload files or paste text first")

    # Compute quality report and field stats
    data_model["quality_report"] = _build_quality_report(data_model)
    data_model["field_stats"] = _build_field_stats(data_model)

    # Save to job directory
    output_path = job_path / "data_model.json"
    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(data_model, f, indent=2, ensure_ascii=False)

    logger.info(
        "Saved data_model.json for job %s (%d products, strategy=%s)",
        job_id,
        len(data_model.get("products", [])),
        data_model.get("matching_strategy", "unknown"),
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
            sample, all_images, conversation_history, data_format="csv",
        )
        if not script:
            raise ValueError("LLM did not produce an extraction script")

    # Step 2: Run server-side, validate, iterate on errors
    products = await _run_and_validate_script(
        script, full_csv, sample["total_rows"], all_images,
        sample, conversation_history, data_format="csv",
    )

    # Save the working script for reuse
    _save_extraction_script(job_path, script, col_fingerprint, sample["headers"])

    return _assemble_data_model(
        products, all_images, sheet_filename,
        sample["headers"], sample["total_rows"],
    )


# ---------------------------------------------------------------------------
# JSON extraction via LLM-generated script (same pipeline, different format)
# ---------------------------------------------------------------------------


async def _build_json_data_model(
    job_path,
    json_path,
    json_filename: str,
    all_images: list[str],
    conversation_history: list[dict],
) -> dict:
    """Extract products from a JSON file using the code execution pipeline."""
    sample = read_json_sample(json_path)
    full_json = read_full_json(json_path)
    col_fingerprint = _column_fingerprint(sample["headers"])

    script = _load_saved_script(job_path, col_fingerprint)
    if script:
        logger.info("Reusing saved JSON extraction script (fingerprint %s)", col_fingerprint)
    else:
        script, _ = await _develop_extraction_script(
            sample, all_images, conversation_history, data_format="json",
        )
        if not script:
            raise ValueError("LLM did not produce a JSON extraction script")

    products = await _run_and_validate_script(
        script, full_json, sample["total_rows"], all_images,
        sample, conversation_history, data_format="json",
    )

    _save_extraction_script(job_path, script, col_fingerprint, sample["headers"])

    return _assemble_data_model(
        products, all_images, json_filename,
        sample["headers"], sample["total_rows"],
    )


# ---------------------------------------------------------------------------
# Pasted text extraction — direct LLM
# ---------------------------------------------------------------------------


async def _build_paste_data_model(
    pasted_text: str,
    all_images: list[str],
    conversation_history: list[dict],
) -> dict:
    """Extract products from small pasted text via direct LLM generation.

    The full text fits in context, so we ask the LLM to output structured
    JSON directly rather than writing a script.
    """
    transcript = "\n".join(
        f"{'User' if m.get('role') == 'user' else 'Assistant'}: {m.get('content', '')}"
        for m in conversation_history[-6:]
    )

    image_list = "\n".join(f"  - {name}" for name in all_images[:50])
    if len(all_images) > 50:
        image_list += f"\n  ... and {len(all_images) - 50} more"

    prompt = f"""\
You are extracting structured product data from user-provided text.

## User's Text
{pasted_text}

## Context from Conversation
{transcript}

## Image Files ({len(all_images)} total)
{image_list}

## Instructions
Extract EVERY product or item mentioned in the text. For each product,
identify all available attributes (name, price, category, color, material,
size, condition, description, etc.).

If images are available, try to match them to products by looking for
patterns in the filenames (SKU, name, numbering).

Return a JSON object with EXACTLY this structure:
{{
  "fields_discovered": ["field1", "field2", ...],
  "products": [
    {{
      "id": "product_001",
      "source": "pasted_text",
      "image_files": [],
      ... all discovered fields as key-value pairs ...
    }}
  ],
  "image_matching_strategy": "description of how images were matched"
}}

IMPORTANT:
- Every product must have a unique "id" (product_001, product_002, etc.)
- Set "source" to "pasted_text" for all products
- Extract ALL products — do not skip any
- Parse structured data (JSON, CSV, tables) if present in the text
- For freeform descriptions, extract what you can"""

    response = await generate_with_text(prompt, thinking_level="high")
    result = _parse_json_from_response(response)

    products = result.get("products", [])
    fields = result.get("fields_discovered", [])

    # Build unmatched images list
    matched_images = set()
    for p in products:
        for img in p.get("image_files", []):
            matched_images.add(img)
    unmatched = [img for img in all_images if img not in matched_images]

    return {
        "sources": {
            "pasted_text": {
                "length": len(pasted_text),
                "type": "direct_extraction",
            },
            "images": {
                "total": len(all_images),
                "matched": len(matched_images),
                "unmatched": unmatched,
            },
        },
        "fields_discovered": sorted(fields) if fields else sorted(
            k for k in (products[0].keys() if products else [])
            if k not in ("id", "source", "image_files")
        ),
        "products": products,
        "unmatched_images": [
            {"filename": f, "status": "unmatched"} for f in unmatched
        ],
        "matching_strategy": result.get(
            "image_matching_strategy", "Direct LLM extraction from pasted text"
        ),
    }


async def _build_large_paste_data_model(
    job_path,
    pasted_text: str,
    all_images: list[str],
    conversation_history: list[dict],
) -> dict:
    """Handle large pasted text by converting to a data file and using code execution.

    Saves the text to a temp file and runs it through the script-based pipeline,
    similar to spreadsheet extraction.
    """
    import tempfile

    # Save pasted text to a temp CSV-like file for the code execution pipeline
    temp_path = job_path / "uploads" / "_pasted_input.txt"
    temp_path.write_text(pasted_text, encoding="utf-8")

    # Create a pseudo-sample for the LLM to analyze
    lines = pasted_text.splitlines()
    total_lines = len(lines)

    # Build a sample from the text (first 10 + random 5 + last 5 lines)
    if total_lines <= 20:
        sample_text = pasted_text
    else:
        import random as _random
        head = lines[:10]
        tail = lines[-5:]
        middle_pool = list(range(10, total_lines - 5))
        middle_idx = sorted(_random.sample(middle_pool, min(5, len(middle_pool))))
        middle = [lines[i] for i in middle_idx]
        sample_text = "\n".join(head + middle + tail)

    sample = {
        "headers": [],
        "sample_csv": sample_text,  # Not CSV but the script will handle raw text
        "total_rows": total_lines,
    }

    transcript = "\n".join(
        f"{'User' if m.get('role') == 'user' else 'Assistant'}: {m.get('content', '')}"
        for m in conversation_history[-6:]
    )

    image_list = "\n".join(f"  - {name}" for name in all_images[:50])

    prompt = f"""\
You are building a data extraction script for product data that was pasted as text.

## Sample of Pasted Text (from {total_lines} total lines)
The attached text contains a representative sample of the full input.

## Image Files ({len(all_images)} total)
{image_list}

## Context
{transcript}

## Your Task
Write a Python script that:

1. Reads the raw text from a string variable called `csv_data`
   (despite the name, this is raw pasted text, not necessarily CSV).

2. Parses the text to identify individual products. The text might be:
   - Tab/comma separated data
   - JSON data
   - Freeform product descriptions separated by blank lines
   - A numbered/bulleted list

3. Extracts product attributes (name, price, category, etc.)

4. Tries to match image files from `image_filenames` list.

5. Assigns the result to `result_json` as a JSON string with structure:
   {{
     "fields_discovered": ["field1", "field2", ...],
     "products": [
       {{
         "id": "product_001",
         "image_files": [],
         "source": "pasted_text_line_N",
         ... discovered fields ...
       }}
     ],
     "image_matching_strategy": "description"
   }}

IMPORTANT:
- The script MUST work on the full text, not just this sample
- Do NOT hardcode values from the sample
- Handle edge cases: empty lines, mixed formatting"""

    _, script = await generate_code_execution_with_parts(
        prompt,
        csv_data=sample["sample_csv"],
        thinking_level="high",
    )

    if not script:
        raise ValueError("LLM did not produce a script for large pasted text")

    products = await _run_and_validate_script(
        script, pasted_text, total_lines, all_images,
        sample, conversation_history, data_format="csv",
    )

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
            "pasted_text": {
                "length": len(pasted_text),
                "type": "code_execution",
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
        "matching_strategy": "Code execution extraction from large pasted text",
    }


# ---------------------------------------------------------------------------
# Vision extraction for image-only uploads
# ---------------------------------------------------------------------------


async def _build_vision_data_model(
    job_id: str,
    all_images: list[str],
    conversation_history: list[dict],
) -> dict:
    """Extract product data from images using LLM vision.

    Processes images in batches to stay within token limits. Each batch
    is sent to the LLM which describes what it sees in each photo.
    """
    uploads_dir = get_job_path(job_id) / "uploads"

    transcript = "\n".join(
        f"{'User' if m.get('role') == 'user' else 'Assistant'}: {m.get('content', '')}"
        for m in conversation_history[-6:]
    )

    all_products = []

    for batch_start in range(0, len(all_images), VISION_BATCH_SIZE):
        batch = all_images[batch_start:batch_start + VISION_BATCH_SIZE]

        image_parts = []
        valid_filenames = []
        for img_name in batch:
            img_path = uploads_dir / img_name
            if img_path.exists():
                try:
                    img_bytes, mime_type = load_image_as_bytes(img_path)
                    image_parts.append((img_bytes, mime_type))
                    valid_filenames.append(img_name)
                except Exception as e:
                    logger.warning("Vision: failed to load %s: %s", img_name, e)

        if not image_parts:
            continue

        file_list = "\n".join(
            f"  {i + 1}. {name}" for i, name in enumerate(valid_filenames)
        )

        prompt = f"""\
You are analyzing product photos to extract attributes for marketplace listings.

## Images
The following {len(valid_filenames)} images are attached (in order):
{file_list}

## Context
{transcript}

## Instructions
For EACH image, extract as many product attributes as you can identify:
- category (e.g., "lamp", "vase", "jewelry", "clothing")
- description_hints (brief description of what you see)
- color (primary colors)
- material (if identifiable)
- condition (new, used, vintage, etc.)
- style (modern, vintage, art deco, mid-century, bohemian, etc.)
- notable_features (list of distinctive features)
- any other attributes visible in the photo

Return a JSON object:
{{
  "products": [
    {{
      "image_filename": "the_filename.jpg",
      "category": "...",
      "description_hints": "...",
      "color": "...",
      "material": "...",
      "condition": "...",
      "style": "...",
      "notable_features": ["...", "..."]
    }}
  ]
}}

One product per image. Be specific and accurate based on what you see.
If you cannot determine an attribute, omit it rather than guessing."""

        response = await generate_with_images(
            prompt, image_parts, thinking_level="medium",
        )
        try:
            result = _parse_json_from_response(response)
            for p in result.get("products", []):
                all_products.append(p)
        except ValueError:
            logger.warning("Vision batch failed to parse, skipping %d images", len(batch))

    # Assemble into standard data model
    products = []
    fields = set()
    for i, vp in enumerate(all_products, 1):
        product = {
            "id": f"product_{i:03d}",
            "image_files": [vp.get("image_filename", all_images[i - 1] if i <= len(all_images) else "unknown")],
            "source": "vision_extraction",
        }
        for key, value in vp.items():
            if key != "image_filename" and value:
                if isinstance(value, list):
                    product[key] = ", ".join(str(v) for v in value)
                else:
                    product[key] = value
                fields.add(key)
        products.append(product)

    # If we got fewer products than images (some batches failed), add bare entries
    if len(products) < len(all_images):
        for i in range(len(products) + 1, len(all_images) + 1):
            products.append({
                "id": f"product_{i:03d}",
                "image_files": [all_images[i - 1]],
                "source": "vision_extraction",
            })

    return {
        "sources": {
            "images": {
                "total": len(all_images),
                "matched": len(all_images),
                "unmatched": [],
            }
        },
        "fields_discovered": sorted(fields),
        "products": products,
        "unmatched_images": [],
        "matching_strategy": "Vision-based extraction from product images",
    }


async def _develop_extraction_script(
    sample: dict,
    all_images: list[str],
    conversation_history: list[dict],
    data_format: str = "csv",
) -> tuple[str | None, str]:
    """Ask the LLM to write an extraction script using code_execution.

    Supports both CSV and JSON input formats. The sample data is attached
    as an inline file part so the LLM can test its code inside the sandbox.

    Args:
        sample: Sample data dict with headers, sample content, and total_rows.
        all_images: List of image filenames.
        conversation_history: Recent chat messages.
        data_format: "csv" for spreadsheets, "json" for JSON files.

    Returns (script_code, llm_text_response).
    """
    transcript = "\n".join(
        f"{'User' if m.get('role') == 'user' else 'Assistant'}: {m.get('content', '')}"
        for m in conversation_history[-6:]
    )

    image_list = "\n".join(f"  - {name}" for name in all_images[:50])
    if len(all_images) > 50:
        image_list += f"\n  ... and {len(all_images) - 50} more"

    data_var = "json_data" if data_format == "json" else "csv_data"
    sample_data = sample.get("sample_json") if data_format == "json" else sample.get("sample_csv")

    if data_format == "json":
        array_key = sample.get("array_key")
        read_step = (
            f"1. Reads JSON from a string variable called `{data_var}` using:\n"
            f"   `data = json.loads({data_var})`\n"
        )
        if array_key:
            read_step += f"   The product array is under the key \"{array_key}\".\n"
        format_label = "JSON"
        source_prefix = "json_item"
    else:
        read_step = (
            f"1. Reads a CSV from a string variable called `{data_var}` using:\n"
            f"   `df = pd.read_csv(io.StringIO({data_var}))`\n"
        )
        format_label = "CSV"
        source_prefix = "spreadsheet_row"

    prompt = f"""\
You are building a data extraction script for a product catalog.

## Sample Data ({format_label})
The attached data contains a representative sample of the full dataset \
({sample['total_rows']} total items). Use it to develop and test your script.

## Image Files ({len(all_images)} total)
{image_list}

## Context from User Conversation
{transcript}

## Your Task
Write a Python script that (IMPORTANT: never use open() — all data is pre-loaded in string variables):

{read_step}
2. Cleans and normalizes the data:
   - Strip whitespace from all string values
   - Handle missing/null values (replace with None, not "nan" or "null")
   - Parse prices to float if a price field exists (handle currency symbols)
   - Drop fully empty entries

3. Discovers all meaningful product fields.

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
         "source": "{source_prefix}_1",
         ... all discovered fields as key-value pairs ...
       }}
     ],
     "image_matching_strategy": "description of how images were matched"
   }}
   ```

IMPORTANT RULES:
- The script MUST work on both the sample now AND the full dataset later.
- Use `{data_var}` as input (a string). Use `image_filenames` (a list of strings).
- Assign the final JSON string to `result_json`.
- Do NOT hardcode row counts or values from the sample.
- Handle edge cases: empty cells, mixed types, encoding oddities.
- Every product must have a unique "id" (e.g., "product_001", "product_002").

Test your script on the attached data. Print a summary showing item count and fields."""

    text_response, script = await generate_code_execution_with_parts(
        prompt,
        csv_data=sample_data,
        thinking_level="high",
    )
    return script, text_response


async def _run_and_validate_script(
    script: str,
    full_data: str,
    expected_rows: int,
    all_images: list[str],
    sample: dict,
    conversation_history: list[dict],
    data_format: str = "csv",
) -> list[dict]:
    """Run extraction script server-side, validate, fix if needed.

    Iterates up to MAX_EXTRACTION_RETRIES times on failure.
    """
    current_script = script

    for attempt in range(MAX_EXTRACTION_RETRIES):
        products, errors = _execute_extraction_script(
            current_script, full_data, all_images, expected_rows,
            data_format=data_format,
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
                data_format=data_format,
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
    full_data: str,
    image_filenames: list[str],
    expected_rows: int,
    data_format: str = "csv",
) -> tuple[list[dict], list[str]]:
    """Execute the LLM-generated script in a restricted sandbox with pandas.

    Supports both CSV data (via csv_data variable) and JSON data (via
    json_data variable) depending on data_format.

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
        "image_filenames": image_filenames,
        "result_json": None,
    }

    # Provide data under the variable name the script expects
    if data_format == "json":
        sandbox_locals["json_data"] = full_data
    else:
        sandbox_locals["csv_data"] = full_data

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
            f"but data source has {expected_rows} rows"
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
    data_format: str = "csv",
) -> str | None:
    """Send the broken script + errors back to the LLM for a fix."""
    error_text = "\n".join(f"- {e}" for e in errors)
    data_var = "json_data" if data_format == "json" else "csv_data"
    sample_data = sample.get("sample_json") if data_format == "json" else sample.get("sample_csv")

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
- Read from `{data_var}` (string variable already in scope) and `image_filenames` (list)
- Assign JSON string to `result_json`
- NEVER use open() — data is already provided as a string variable.
  For CSV: `df = pd.read_csv(io.StringIO({data_var}))`
  For JSON: `data = json.loads({data_var})`

Test on the attached sample data, then print a summary."""

    text_response, fixed_script = await generate_code_execution_with_parts(
        prompt,
        csv_data=sample_data,
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
