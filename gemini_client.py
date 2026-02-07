"""
Shared Gemini SDK configuration and helper functions.

Provides configured client, model constants, tool definitions, and
convenience functions for different generation patterns (text-only,
multimodal, code execution, search, structured output).
"""

import os
from typing import Optional
from dotenv import load_dotenv
from google import genai
from google.genai import types

load_dotenv()

# Environment configuration
GEMINI_API_KEY = os.getenv("GEMINI_API_KEY", "")

BATCH_MODEL = os.getenv("BATCH_MODEL", "gemini-3-flash-preview")
_use_pro = os.getenv("USE_PRO", "true").lower() in ("true", "1", "yes")
REASONING_MODEL = os.getenv("REASONING_MODEL", "gemini-3-pro-preview" if _use_pro else BATCH_MODEL)

# Initialize client (lazy — will fail on first API call if key is missing)
client = genai.Client(api_key=GEMINI_API_KEY) if GEMINI_API_KEY else None


def _ensure_client():
    if client is None:
        raise ValueError("GEMINI_API_KEY environment variable is required")
    return client

# Tool definitions
# CRITICAL: code_execution and google_search CANNOT be used together
CODE_EXECUTION_TOOL = types.Tool(code_execution=types.ToolCodeExecution())
GOOGLE_SEARCH_TOOL = types.Tool(google_search=types.GoogleSearch())


def _valid_thinking_level(level: str) -> str:
    """Validate thinking level string for Gemini 3 models."""
    valid = {"high", "medium", "low", "minimal"}
    level = level.lower()
    return level if level in valid else "high"


async def generate_with_text(
    prompt: str,
    *,
    model: Optional[str] = None,
    thinking_level: str = "high"
) -> str:
    """
    Simple text-only generation.

    Args:
        prompt: The text prompt
        model: Model name (defaults to REASONING_MODEL)
        thinking_level: "high", "medium", or "low"

    Returns:
        Text response from the model
    """
    model_name = model or REASONING_MODEL

    config = types.GenerateContentConfig(
        thinking_config=types.ThinkingConfig(
            thinking_level=_valid_thinking_level(thinking_level)
        )
    )

    response = await _ensure_client().aio.models.generate_content(
        model=model_name,
        contents=prompt,
        config=config
    )

    return response.text or ""


async def generate_with_images(
    prompt: str,
    image_parts: list,
    *,
    model: Optional[str] = None,
    thinking_level: str = "high"
) -> str:
    """
    Multimodal generation with images.

    Args:
        prompt: The text prompt
        image_parts: List of (bytes, mime_type) tuples
        model: Model name (defaults to REASONING_MODEL)
        thinking_level: "high", "medium", or "low"

    Returns:
        Text response from the model
    """
    model_name = model or REASONING_MODEL

    # Build content parts: text prompt followed by images
    parts = [types.Part.from_text(text=prompt)]
    for img_bytes, mime_type in image_parts:
        parts.append(types.Part.from_bytes(data=img_bytes, mime_type=mime_type))

    config = types.GenerateContentConfig(
        thinking_config=types.ThinkingConfig(
            thinking_level=_valid_thinking_level(thinking_level)
        )
    )

    response = await _ensure_client().aio.models.generate_content(
        model=model_name,
        contents=parts,
        config=config
    )

    return response.text or ""


async def generate_with_code_execution(
    prompt: str,
    image_parts: Optional[list] = None,
    *,
    model: Optional[str] = None,
    thinking_level: str = "high"
) -> str:
    """
    Generation with code_execution tool enabled.
    For discovery and recipe testing.

    Args:
        prompt: The text prompt
        image_parts: Optional list of (bytes, mime_type) tuples
        model: Model name (defaults to REASONING_MODEL)
        thinking_level: "high", "medium", or "low"

    Returns:
        Text response from the model
    """
    model_name = model or REASONING_MODEL

    # Build content parts
    parts = [types.Part.from_text(text=prompt)]
    if image_parts:
        for img_bytes, mime_type in image_parts:
            parts.append(types.Part.from_bytes(data=img_bytes, mime_type=mime_type))

    config = types.GenerateContentConfig(
        tools=[CODE_EXECUTION_TOOL],
        thinking_config=types.ThinkingConfig(
            thinking_level=_valid_thinking_level(thinking_level)
        )
    )

    response = await _ensure_client().aio.models.generate_content(
        model=model_name,
        contents=parts,
        config=config
    )

    return response.text or ""


async def generate_with_search(
    prompt: str,
    image_parts: Optional[list] = None,
    *,
    model: Optional[str] = None,
    thinking_level: str = "low"
) -> str:
    """
    Generation with google_search tool enabled.
    For batch execution pricing research.

    Args:
        prompt: The text prompt
        image_parts: Optional list of (bytes, mime_type) tuples
        model: Model name (defaults to BATCH_MODEL)
        thinking_level: "high", "medium", or "low"

    Returns:
        Text response from the model
    """
    model_name = model or BATCH_MODEL

    # Build content parts
    parts = [types.Part.from_text(text=prompt)]
    if image_parts:
        for img_bytes, mime_type in image_parts:
            parts.append(types.Part.from_bytes(data=img_bytes, mime_type=mime_type))

    config = types.GenerateContentConfig(
        tools=[GOOGLE_SEARCH_TOOL],
        thinking_config=types.ThinkingConfig(
            thinking_level=_valid_thinking_level(thinking_level)
        )
    )

    response = await _ensure_client().aio.models.generate_content(
        model=model_name,
        contents=parts,
        config=config
    )

    return response.text or ""


async def generate_structured(
    prompt: str,
    image_parts: Optional[list] = None,
    schema: Optional[dict] = None,
    *,
    model: Optional[str] = None,
    thinking_level: str = "low"
) -> dict:
    """
    Generation with structured JSON output.

    Args:
        prompt: The text prompt
        image_parts: Optional list of (bytes, mime_type) tuples
        schema: JSON schema for structured output
        model: Model name (defaults to BATCH_MODEL)
        thinking_level: "high", "medium", or "low"

    Returns:
        Parsed JSON dictionary
    """
    model_name = model or BATCH_MODEL

    # Build content parts
    parts = [types.Part.from_text(text=prompt)]
    if image_parts:
        for img_bytes, mime_type in image_parts:
            parts.append(types.Part.from_bytes(data=img_bytes, mime_type=mime_type))

    config = types.GenerateContentConfig(
        response_mime_type="application/json",
        response_schema=schema,
        thinking_config=types.ThinkingConfig(
            thinking_level=_valid_thinking_level(thinking_level)
        )
    )

    response = await _ensure_client().aio.models.generate_content(
        model=model_name,
        contents=parts,
        config=config
    )

    import json
    return json.loads(response.text) if response.text else {}
