"""File management utilities for ListingAgent.

Handles job directory creation, file categorization, spreadsheet reading,
image processing, and output packaging.
"""

from pathlib import Path
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
DOCUMENT_EXTENSIONS = {".pdf", ".doc", ".docx"}


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

    categories = {"images": [], "spreadsheets": [], "documents": [], "other": []}

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
