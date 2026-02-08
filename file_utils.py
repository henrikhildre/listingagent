"""File management utilities for ListingAgent.

Handles job directory creation, file categorization, spreadsheet reading,
image processing, and output packaging.
"""

from pathlib import Path
import random
import shutil
import zipfile
from typing import Tuple
from PIL import Image
import openpyxl
import pandas as pd
import io

JOB_ROOT = Path("/tmp/jobs")

IMAGE_EXTENSIONS = {".jpg", ".jpeg", ".png", ".webp", ".gif"}
SPREADSHEET_EXTENSIONS = {".xlsx", ".xls", ".csv", ".tsv"}
JSON_EXTENSIONS = {".json"}
DOCUMENT_EXTENSIONS = {".pdf", ".doc", ".docx"}

PASTED_TEXT_FILENAME = "user_input.txt"
MAX_PASTE_LENGTH = 200_000  # characters


def create_job_directory(job_id: str) -> Path:
    """Creates the job directory structure:
    /tmp/jobs/{job_id}/
    ├── uploads/
    ├── images/
    └── output/
        └── listings/
    Returns the job root path.
    """
    job_path = JOB_ROOT / job_id
    job_path.mkdir(parents=True, exist_ok=True)

    (job_path / "uploads").mkdir(exist_ok=True)
    (job_path / "images").mkdir(exist_ok=True)
    (job_path / "output").mkdir(exist_ok=True)
    (job_path / "output" / "listings").mkdir(exist_ok=True)

    return job_path


def get_job_path(job_id: str) -> Path:
    """Returns the job root path."""
    return JOB_ROOT / job_id


def categorize_files(job_id: str) -> dict:
    """Scans uploads directory, categorizes by extension.
    Returns FileCategory-compatible dict:
    {"images": [...], "spreadsheets": [...], "documents": [...], "other": [...]}
    Each list contains filenames (not full paths).
    """
    uploads_dir = get_job_path(job_id) / "uploads"

    categories = {
        "images": [],
        "spreadsheets": [],
        "json_files": [],
        "documents": [],
        "other": [],
    }

    if not uploads_dir.exists():
        return categories

    for filepath in uploads_dir.iterdir():
        if not filepath.is_file():
            continue

        ext = filepath.suffix.lower()
        filename = filepath.name

        if ext in IMAGE_EXTENSIONS:
            categories["images"].append(filename)
        elif ext in SPREADSHEET_EXTENSIONS:
            categories["spreadsheets"].append(filename)
        elif ext in JSON_EXTENSIONS:
            categories["json_files"].append(filename)
        elif ext in DOCUMENT_EXTENSIONS:
            categories["documents"].append(filename)
        else:
            categories["other"].append(filename)

    return categories


def read_spreadsheet_preview(filepath: Path, max_rows: int = 5) -> dict:
    """Reads Excel/CSV and returns headers + sample rows.
    Uses openpyxl for .xlsx, pandas for .csv/.tsv
    Returns: {"headers": [...], "rows": [[...], ...], "total_rows": int}
    """
    ext = filepath.suffix.lower()

    if ext == ".xlsx":
        workbook = openpyxl.load_workbook(filepath, read_only=True, data_only=True)
        sheet = workbook.active

        rows_list = list(sheet.iter_rows(values_only=True))
        if not rows_list:
            return {"headers": [], "rows": [], "total_rows": 0}

        headers = [str(cell) if cell is not None else "" for cell in rows_list[0]]
        data_rows = []

        for row in rows_list[1 : max_rows + 1]:
            data_rows.append([str(cell) if cell is not None else "" for cell in row])

        workbook.close()

        return {"headers": headers, "rows": data_rows, "total_rows": len(rows_list) - 1}

    elif ext in {".csv", ".tsv"}:
        separator = "\t" if ext == ".tsv" else ","
        df = pd.read_csv(filepath, sep=separator, nrows=max_rows)

        headers = df.columns.tolist()
        rows = df.astype(str).values.tolist()

        total_rows = sum(1 for _ in open(filepath, "r", encoding="utf-8")) - 1

        return {"headers": headers, "rows": rows, "total_rows": total_rows}

    elif ext == ".xls":
        df = pd.read_excel(filepath, nrows=max_rows)
        headers = df.columns.tolist()
        rows = df.astype(str).values.tolist()

        df_full = pd.read_excel(filepath)
        total_rows = len(df_full)

        return {"headers": headers, "rows": rows, "total_rows": total_rows}

    raise ValueError(f"Unsupported spreadsheet format: {ext}")


def convert_spreadsheet_to_csv(filepath: Path) -> Path:
    """Convert xlsx/xls/tsv to CSV. CSV files are returned as-is.

    The converted CSV is written alongside the original file.
    """
    ext = filepath.suffix.lower()
    if ext == ".csv":
        return filepath

    csv_path = filepath.with_suffix(".csv")
    if csv_path.exists():
        return csv_path

    if ext == ".tsv":
        df = pd.read_csv(filepath, sep="\t")
    else:
        df = pd.read_excel(filepath)

    df.to_csv(csv_path, index=False)
    return csv_path


def read_spreadsheet_sample(filepath: Path, max_sample: int = 15) -> dict:
    """Read a strategic sample of rows for LLM script development.

    Selects first 5, last 5, and up to 5 random middle rows to give the
    LLM a representative view of data variations.

    Returns: {"headers": [...], "sample_csv": "...", "total_rows": int}
    """
    csv_path = convert_spreadsheet_to_csv(filepath)
    df = pd.read_csv(csv_path)
    total_rows = len(df)

    if total_rows <= max_sample:
        sample_df = df
    else:
        head = df.head(5)
        tail = df.tail(5)
        middle_pool = range(5, total_rows - 5)
        middle_indices = sorted(random.sample(
            list(middle_pool), min(5, len(middle_pool))
        ))
        middle = df.iloc[middle_indices]
        sample_df = pd.concat([head, middle, tail]).drop_duplicates()

    return {
        "headers": df.columns.tolist(),
        "sample_csv": sample_df.to_csv(index=False),
        "total_rows": total_rows,
    }


def read_full_csv(filepath: Path) -> str:
    """Read the full spreadsheet as a CSV string for server-side script execution.

    Converts xlsx/xls/tsv to CSV first if needed.
    """
    csv_path = convert_spreadsheet_to_csv(filepath)
    return csv_path.read_text(encoding="utf-8")


def load_image_as_bytes(filepath: Path) -> Tuple[bytes, str]:
    """Returns (image_bytes, mime_type) for Gemini API.
    Resize if larger than 1024px on longest side to save tokens.
    """
    img = Image.open(filepath)

    max_dimension = max(img.size)
    if max_dimension > 1024:
        scale_factor = 1024 / max_dimension
        new_size = (int(img.size[0] * scale_factor), int(img.size[1] * scale_factor))
        img = img.resize(new_size, Image.Resampling.LANCZOS)

    output_buffer = io.BytesIO()

    img_format = img.format or "PNG"
    if img_format == "JPEG" or filepath.suffix.lower() in {".jpg", ".jpeg"}:
        if img.mode in ("RGBA", "LA", "P"):
            img = img.convert("RGB")
        img.save(output_buffer, format="JPEG", quality=85)
        mime_type = "image/jpeg"
    elif img_format == "PNG" or filepath.suffix.lower() == ".png":
        img.save(output_buffer, format="PNG")
        mime_type = "image/png"
    elif img_format == "WEBP" or filepath.suffix.lower() == ".webp":
        img.save(output_buffer, format="WEBP", quality=85)
        mime_type = "image/webp"
    elif img_format == "GIF" or filepath.suffix.lower() == ".gif":
        img.save(output_buffer, format="GIF")
        mime_type = "image/gif"
    else:
        img.save(output_buffer, format="PNG")
        mime_type = "image/png"

    image_bytes = output_buffer.getvalue()
    output_buffer.close()

    return image_bytes, mime_type


def create_output_zip(job_id: str) -> Path:
    """Packages output directory into a downloadable ZIP.
    Returns path to the ZIP file at /tmp/jobs/{job_id}/output.zip
    """
    job_path = get_job_path(job_id)
    output_dir = job_path / "output"
    zip_path = job_path / "output.zip"

    if zip_path.exists():
        zip_path.unlink()

    with zipfile.ZipFile(zip_path, "w", zipfile.ZIP_DEFLATED) as zipf:
        for file_path in output_dir.rglob("*"):
            if file_path.is_file():
                arcname = file_path.relative_to(output_dir)
                zipf.write(file_path, arcname)

    return zip_path


def cleanup_job(job_id: str):
    """Removes entire job directory."""
    job_path = get_job_path(job_id)
    if job_path.exists():
        shutil.rmtree(job_path)


# ---------------------------------------------------------------------------
# JSON file reading
# ---------------------------------------------------------------------------

import json as _json


def read_json_preview(filepath: Path, max_items: int = 5) -> dict:
    """Read a JSON file and return a preview similar to spreadsheet preview.

    Handles JSON arrays of objects (most common for product data), nested
    objects with an array field, and single objects.

    Returns: {"headers": [...], "rows": [[...], ...], "total_rows": int,
              "format": "json_array"|"json_nested"|"json_object"}
    """
    with open(filepath, encoding="utf-8") as f:
        data = _json.load(f)

    items, array_key = _find_json_items(data)

    if items is None:
        if isinstance(data, dict):
            headers = list(data.keys())
            rows = [[str(data.get(h, "")) for h in headers]]
            return {
                "headers": headers,
                "rows": rows,
                "total_rows": 1,
                "format": "json_object",
            }
        return {"headers": [], "rows": [], "total_rows": 0, "format": "unknown"}

    total = len(items)
    sample = items[:max_items]
    headers = _json_headers(sample)
    rows = [[str(item.get(h, "")) for h in headers] for item in sample if isinstance(item, dict)]

    fmt = "json_nested" if array_key else "json_array"
    result = {"headers": headers, "rows": rows, "total_rows": total, "format": fmt}
    if array_key:
        result["array_key"] = array_key
    return result


def read_json_sample(filepath: Path, max_sample: int = 15) -> dict:
    """Read a strategic sample from a JSON file for LLM script development.

    Same sampling strategy as spreadsheets: first 5, random middle 5, last 5.
    Returns: {"headers": [...], "sample_json": "...", "total_rows": int}
    """
    with open(filepath, encoding="utf-8") as f:
        data = _json.load(f)

    items, array_key = _find_json_items(data)

    if items is None:
        return {
            "headers": list(data.keys()) if isinstance(data, dict) else [],
            "sample_json": _json.dumps(data, ensure_ascii=False, indent=2),
            "total_rows": 1 if isinstance(data, dict) else 0,
            "array_key": array_key,
        }

    total = len(items)
    if total <= max_sample:
        sample = items
    else:
        head = items[:5]
        tail = items[-5:]
        middle_pool = list(range(5, total - 5))
        middle_indices = sorted(random.sample(middle_pool, min(5, len(middle_pool))))
        middle = [items[i] for i in middle_indices]
        sample = head + middle + tail

    headers = _json_headers(sample)
    return {
        "headers": headers,
        "sample_json": _json.dumps(sample, ensure_ascii=False, indent=2),
        "total_rows": total,
        "array_key": array_key,
    }


def read_full_json(filepath: Path) -> str:
    """Read the full JSON file as a string for server-side script execution."""
    return filepath.read_text(encoding="utf-8")


def _find_json_items(data) -> tuple[list | None, str | None]:
    """Find the product array in a JSON structure.

    Returns (items_list_or_None, array_key_or_None).
    """
    if isinstance(data, list):
        return data, None
    if isinstance(data, dict):
        for key, value in data.items():
            if isinstance(value, list) and value and isinstance(value[0], dict):
                return value, key
    return None, None


def _json_headers(items: list) -> list[str]:
    """Collect ordered unique keys from a list of dicts."""
    seen = set()
    headers = []
    for item in items:
        if isinstance(item, dict):
            for key in item:
                if key not in seen:
                    seen.add(key)
                    headers.append(key)
    return headers


# ---------------------------------------------------------------------------
# Pasted text helpers
# ---------------------------------------------------------------------------


def save_pasted_text(job_id: str, text: str) -> Path:
    """Save user-pasted text to the job directory."""
    job_path = get_job_path(job_id)
    filepath = job_path / PASTED_TEXT_FILENAME
    filepath.write_text(text, encoding="utf-8")
    return filepath


def get_pasted_text(job_id: str) -> str | None:
    """Read pasted text from the job directory, or None if not present."""
    filepath = get_job_path(job_id) / PASTED_TEXT_FILENAME
    if filepath.exists():
        return filepath.read_text(encoding="utf-8")
    return None
