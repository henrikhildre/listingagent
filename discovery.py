"""
Phase 1: Data Understanding — explores uploaded files and builds a data model
WITH the user.

Supports three upload scenarios:
  1. Just images (each image = one product)
  2. Excel/CSV + images (most common real scenario)
  3. Images with descriptive filenames (extract info from names)

Key outputs:
  - File categorization summary
  - LLM-driven data analysis (with code_execution for spreadsheets)
  - data_model.json saved to the job directory
"""

import json
import logging
import re
from pathlib import Path

from file_utils import (
    categorize_files,
    get_job_path,
    load_image_as_bytes,
    read_spreadsheet_preview,
)
from gemini_client import generate_with_code_execution, generate_with_text

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
        parts.append(
            f"{n_sheets} spreadsheet{'s' if n_sheets != 1 else ''}{row_info}"
        )
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
        rows_str = "\n".join(
            " | ".join(row) for row in preview["rows"]
        )
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
        prompt_sections.append(
            f"## Image Files ({len(images)} total)\n{image_list}\n"
        )

    # Prior conversation context
    if conversation_history:
        history_text = "\n".join(
            f"{'User' if m['role'] == 'user' else 'Assistant'}: {m['content']}"
            for m in conversation_history
        )
        prompt_sections.append(
            f"## Previous Conversation\n{history_text}\n"
        )

    prompt_sections.append(
        "## Your Task\n"
        "1. Use code_execution to analyze the data structure — parse headers, "
        "detect data types, find patterns in filenames.\n"
        "2. Figure out how images map to spreadsheet rows (if a spreadsheet exists). "
        "Look for SKU patterns, name matches, or numbering conventions.\n"
        "3. Propose a data model: what fields each product has, how images are "
        "linked, and any ambiguities.\n"
        "4. Ask the user any clarifying questions if needed (e.g., unmatched "
        "images, unclear columns).\n\n"
        "If there is NO spreadsheet, treat each image as a product and extract "
        "any info you can from the filenames (e.g., 'blue-wool-scarf-25cm.jpg' "
        "-> name='Blue Wool Scarf', size='25cm').\n\n"
        "Be concise and conversational. Present your findings clearly."
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


async def build_data_model(
    job_id: str, conversation_history: list[dict]
) -> dict:
    """Generate the structured data_model.json from the discovery conversation.

    Takes the full conversation history (user confirmations, corrections, and
    LLM analysis) and asks Gemini to produce the final data model JSON.
    Saves the result to the job directory.

    Args:
        job_id: The job identifier.
        conversation_history: Full list of {"role": ..., "content": ...} dicts.

    Returns:
        The parsed data model dictionary (also saved as data_model.json).
    """
    job_path = get_job_path(job_id)
    uploads_dir = job_path / "uploads"

    # Re-read file summary for grounding
    file_summary = await categorize_uploads(job_id)

    # Build the conversation transcript
    transcript = "\n".join(
        f"{'User' if m['role'] == 'user' else 'Assistant'}: {m['content']}"
        for m in conversation_history
    )

    # List all images for the model to reference
    all_images = sorted(file_summary.get("images", []))
    images_list = "\n".join(f"  - {name}" for name in all_images)

    # Spreadsheet info for grounding
    sheet_info = ""
    for filename, preview in file_summary.get("spreadsheet_previews", {}).items():
        if "error" in preview:
            continue
        headers_str = ", ".join(preview["headers"])
        sheet_info += (
            f"Spreadsheet '{filename}': {preview['total_rows']} rows, "
            f"columns: [{headers_str}]\n"
        )

    prompt = f"""\
Based on the following discovery conversation, generate the final data model JSON.

## File Summary
{file_summary['summary']}

{sheet_info}
## Image Files ({len(all_images)} total)
{images_list}

## Discovery Conversation
{transcript}

## Instructions
Generate a JSON object with this structure:
{{
  "sources": {{
    "spreadsheet": {{
      "filename": "...",
      "columns": {{"column_name": "description", ...}},
      "row_count": N
    }},
    "images": {{
      "total": N,
      "matched": N,
      "unmatched": ["filename1.jpg", ...],
      "matching_strategy": "Description of how images were matched to products"
    }}
  }},
  "products": [
    {{
      "id": "product_001",
      "sku": "ABC-123" or null,
      "name": "Product Name" or null,
      "category": "Category" or null,
      "price": 12.00 or null,
      "image_files": ["image1.jpg"],
      "metadata": {{}},
      "source": "spreadsheet_row_1" or "image_only"
    }}
  ],
  "unmatched_images": [
    {{
      "filename": "IMG_8821.jpg",
      "status": "pending_user_input",
      "llm_guess": "Appears to be ..."
    }}
  ],
  "matching_strategy": "Description of the matching approach used"
}}

IMPORTANT:
- Include ALL products found in the data.
- Every image should appear either in a product's image_files or in unmatched_images.
- If there is no spreadsheet, each image is its own product. Extract what you can from filenames.
- Use the conversation to resolve any ambiguities the user clarified.
- If the spreadsheet has no "sources.spreadsheet" section, omit it from sources.

Return ONLY the JSON object, wrapped in ```json ... ``` fences. No other text."""

    response_text = await generate_with_text(prompt, thinking_level="high")

    # Parse the JSON from the response
    data_model = _parse_json_from_response(response_text)

    # Save to job directory
    output_path = job_path / "data_model.json"
    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(data_model, f, indent=2, ensure_ascii=False)

    logger.info("Saved data_model.json for job %s (%d products)", job_id, len(data_model.get("products", [])))

    return data_model


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
        f"Could not extract valid JSON from response. "
        f"First 200 chars: {text[:200]}"
    )
