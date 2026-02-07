from datetime import datetime
from typing import Literal

from pydantic import BaseModel, Field


class JobState(BaseModel):
    job_id: str
    phase: Literal[
        "uploading",
        "discovering",
        "interviewing",
        "building_recipe",
        "testing",
        "executing",
        "complete",
    ]
    created_at: datetime


class FileCategory(BaseModel):
    images: list[str]
    spreadsheets: list[str]
    documents: list[str]
    other: list[str]


class Product(BaseModel):
    """Minimal required fields — all other fields are discovered per-job.

    The extraction pipeline produces dicts with dynamic keys (e.g. sku,
    name, material, color) depending on what the spreadsheet contains.
    Only id, image_files, and source are guaranteed.
    """
    id: str
    image_files: list[str] = []
    source: str

    model_config = {"extra": "allow"}


class DataModel(BaseModel):
    sources: dict
    fields_discovered: list[str] = []
    products: list[dict]
    unmatched_images: list[dict] = []
    matching_strategy: str


class StyleProfile(BaseModel):
    platform: str
    seller_type: str
    target_buyer: str
    brand_voice: str
    description_structure: str
    avg_description_length: str
    pricing_strategy: str
    tags_style: str
    title_format: str
    always_mention: list[str] = []
    example_listings: list[dict] = []


class Recipe(BaseModel):
    version: int = 1
    prompt_template: str
    output_schema: dict
    validation_code: str
    test_results: list[dict] = []
    approved: bool = False


class ListingOutput(BaseModel):
    title: str
    description: str
    tags: list[str]
    category_suggestion: str | None = None
    suggested_price: float
    pricing_rationale: str | None = None
    seo_keywords: list[str] = []
    confidence: Literal["high", "medium", "low"]
    notes_for_seller: str | None = None
    # --- Enhanced fields for resellers ---
    social_caption: str | None = None
    hashtags: list[str] = []
    item_specifics: dict[str, str] = {}
    condition_description: str | None = None


class BatchProgress(BaseModel):
    job_id: str
    total: int
    completed: int
    current_product: str | None = None
    last_score: int | None = None
    errors: int = 0


class ChatMessage(BaseModel):
    role: Literal["user", "assistant"]
    content: str
    timestamp: datetime = Field(default_factory=datetime.utcnow)
