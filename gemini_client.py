"""
Shared Gemini SDK configuration and helper functions.

Provides configured client, model constants, tool definitions, and
convenience functions for different generation patterns (text-only,
multimodal, code execution, search, structured output).
"""

import asyncio
import logging
import os
from functools import wraps
from typing import Optional

from dotenv import load_dotenv
from google import genai
from google.genai import types
from google.genai.errors import APIError, ClientError, ServerError

load_dotenv()

# Environment configuration
GEMINI_API_KEY = os.getenv("GEMINI_API_KEY", "")

BATCH_MODEL = os.getenv("BATCH_MODEL", "gemini-3-flash-preview")
_use_pro = os.getenv("USE_PRO", "true").lower() in ("true", "1", "yes")
REASONING_MODEL = os.getenv(
    "REASONING_MODEL", "gemini-3-pro-preview" if _use_pro else BATCH_MODEL
)

# Initialize client (lazy — will fail on first API call if key is missing)
client = genai.Client(api_key=GEMINI_API_KEY) if GEMINI_API_KEY else None


def _ensure_client():
    if client is None:
        raise ValueError("GEMINI_API_KEY environment variable is required")
    return client


logger = logging.getLogger(__name__)

# Retry configuration
MAX_RETRIES = 3
INITIAL_BACKOFF = 2  # seconds
QUOTA_KEYWORDS = ("quota", "limit exceeded", "daily limit", "rate limit exceeded")


class QuotaExhaustedError(Exception):
    """Raised when the daily API quota is used up (not a transient rate limit)."""


def _is_retryable(error: Exception) -> bool:
    """Return True if the error is a transient failure worth retrying."""
    if isinstance(error, ClientError) and error.code == 429:
        msg = (str(error.message) or "").lower()
        # Daily quota exhaustion is NOT retryable
        if any(kw in msg for kw in QUOTA_KEYWORDS):
            return False
        return True
    if isinstance(error, ServerError):
        return True  # 500/503 are transient
    return False


def _is_quota_error(error: Exception) -> bool:
    """Return True if this is a daily quota exhaustion (stop, don't retry)."""
    if isinstance(error, ClientError) and error.code == 429:
        msg = (str(error.message) or "").lower()
        return any(kw in msg for kw in QUOTA_KEYWORDS)
    return False


def with_retry(fn):
    """Decorator that adds exponential backoff retry for transient API errors."""
    @wraps(fn)
    async def wrapper(*args, **kwargs):
        last_error = None
        for attempt in range(MAX_RETRIES + 1):
            try:
                return await fn(*args, **kwargs)
            except (ClientError, ServerError, APIError) as e:
                last_error = e
                if _is_quota_error(e):
                    raise QuotaExhaustedError(
                        f"Daily Gemini API quota exhausted: {e}"
                    ) from e
                if not _is_retryable(e) or attempt == MAX_RETRIES:
                    raise
                delay = INITIAL_BACKOFF * (2 ** attempt)
                logger.warning(
                    "Gemini API error (attempt %d/%d), retrying in %ds: %s",
                    attempt + 1, MAX_RETRIES + 1, delay, e,
                )
                await asyncio.sleep(delay)
        raise last_error  # should not reach here
    return wrapper


# Tool definitions
# CRITICAL: code_execution and google_search CANNOT be used together
CODE_EXECUTION_TOOL = types.Tool(code_execution=types.ToolCodeExecution())
GOOGLE_SEARCH_TOOL = types.Tool(google_search=types.GoogleSearch())


def _valid_thinking_level(level: str) -> str:
    """Validate thinking level string for Gemini 3 models."""
    valid = {"high", "medium", "low", "minimal"}
    level = level.lower()
    return level if level in valid else "high"


@with_retry
async def generate_with_text(
    prompt: str, *, model: Optional[str] = None, thinking_level: str = "high"
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
        model=model_name, contents=prompt, config=config
    )

    return response.text or ""


@with_retry
async def generate_with_images(
    prompt: str,
    image_parts: list,
    *,
    model: Optional[str] = None,
    thinking_level: str = "high",
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
        model=model_name, contents=parts, config=config
    )

    return response.text or ""


@with_retry
async def generate_with_code_execution(
    prompt: str,
    image_parts: Optional[list] = None,
    *,
    model: Optional[str] = None,
    thinking_level: str = "high",
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
        ),
    )

    response = await _ensure_client().aio.models.generate_content(
        model=model_name, contents=parts, config=config
    )

    return response.text or ""


@with_retry
async def generate_code_execution_with_parts(
    prompt: str,
    csv_data: str | None = None,
    image_parts: Optional[list] = None,
    *,
    model: Optional[str] = None,
    thinking_level: str = "high",
) -> tuple[str, str | None]:
    """Code execution that also returns the last generated script.

    Attaches CSV data as an inline text/csv part so the sandbox can read it.

    Returns:
        (text_response, last_executable_code_or_None)
    """
    model_name = model or REASONING_MODEL

    parts = [types.Part.from_text(text=prompt)]

    if csv_data:
        parts.append(
            types.Part.from_bytes(data=csv_data.encode("utf-8"), mime_type="text/csv")
        )

    if image_parts:
        for img_bytes, mime_type in image_parts:
            parts.append(types.Part.from_bytes(data=img_bytes, mime_type=mime_type))

    config = types.GenerateContentConfig(
        tools=[CODE_EXECUTION_TOOL],
        thinking_config=types.ThinkingConfig(
            thinking_level=_valid_thinking_level(thinking_level)
        ),
    )

    response = await _ensure_client().aio.models.generate_content(
        model=model_name, contents=parts, config=config
    )

    # Walk response parts to extract the last executable_code block
    script = None
    text_parts = []
    if response.candidates and response.candidates[0].content:
        for part in response.candidates[0].content.parts:
            if hasattr(part, "executable_code") and part.executable_code:
                script = part.executable_code.code
            if hasattr(part, "text") and part.text:
                text_parts.append(part.text)

    text_response = "\n".join(text_parts) if text_parts else (response.text or "")
    return text_response, script


@with_retry
async def generate_with_search(
    prompt: str,
    image_parts: Optional[list] = None,
    *,
    model: Optional[str] = None,
    thinking_level: str = "low",
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
        ),
    )

    response = await _ensure_client().aio.models.generate_content(
        model=model_name, contents=parts, config=config
    )

    return response.text or ""


@with_retry
async def generate_structured(
    prompt: str,
    image_parts: Optional[list] = None,
    schema: Optional[dict] = None,
    *,
    model: Optional[str] = None,
    thinking_level: str = "low",
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
        ),
    )

    response = await _ensure_client().aio.models.generate_content(
        model=model_name, contents=parts, config=config
    )

    import json

    return json.loads(response.text) if response.text else {}
