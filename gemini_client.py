"""
Shared Gemini SDK configuration and helper functions.

Provides configured client, model constants, tool definitions, and
convenience functions for different generation patterns (text-only,
multimodal, code execution, search, structured output).
"""

import asyncio
import json
import logging
import os
import re
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

# ---------------------------------------------------------------------------
# Token usage tracking
# ---------------------------------------------------------------------------

# Cost per 1M tokens (USD)
INPUT_COST_PER_M = 0.50
OUTPUT_COST_PER_M = 3.00

_token_usage = {"input": 0, "output": 0, "calls": 0}
_current_step: str | None = None
_step_snapshot: dict | None = None  # snapshot of _token_usage at step start
_step_history: list[dict] = []  # completed step summaries


def _calc_cost(input_tokens: int, output_tokens: int) -> float:
    """Calculate USD cost from token counts."""
    return (input_tokens * INPUT_COST_PER_M + output_tokens * OUTPUT_COST_PER_M) / 1_000_000


def _log_tokens(response, model_name: str, caller: str) -> None:
    """Log token counts from a Gemini response and update running totals."""
    meta = getattr(response, "usage_metadata", None)
    if meta is None:
        return
    inp = getattr(meta, "prompt_token_count", 0) or 0
    out = getattr(meta, "candidates_token_count", 0) or 0
    _token_usage["input"] += inp
    _token_usage["output"] += out
    _token_usage["calls"] += 1
    cost = _calc_cost(inp, out)
    logger.info(
        "TOKENS [%s] %s: in=%d out=%d ($%.4f) | cumulative: in=%d out=%d, calls=%d ($%.4f)",
        model_name, caller, inp, out, cost,
        _token_usage["input"], _token_usage["output"], _token_usage["calls"],
        _calc_cost(_token_usage["input"], _token_usage["output"]),
    )


def start_step(name: str) -> None:
    """Mark the beginning of a workflow step for token tracking."""
    global _current_step, _step_snapshot
    _current_step = name
    _step_snapshot = {
        "input": _token_usage["input"],
        "output": _token_usage["output"],
        "calls": _token_usage["calls"],
    }
    logger.info("COST STEP [%s] started", name)


def end_step(name: str | None = None) -> dict:
    """Mark the end of a workflow step. Logs summary with cost. Returns step stats."""
    global _current_step, _step_snapshot
    step_name = name or _current_step or "unknown"

    if _step_snapshot is None:
        # No matching start_step — compute from zero
        snap_in, snap_out, snap_calls = 0, 0, 0
    else:
        snap_in = _step_snapshot["input"]
        snap_out = _step_snapshot["output"]
        snap_calls = _step_snapshot["calls"]

    step_in = _token_usage["input"] - snap_in
    step_out = _token_usage["output"] - snap_out
    step_calls = _token_usage["calls"] - snap_calls
    step_cost = _calc_cost(step_in, step_out)
    total_cost = _calc_cost(_token_usage["input"], _token_usage["output"])

    summary = {
        "step": step_name,
        "input": step_in,
        "output": step_out,
        "calls": step_calls,
        "cost": round(step_cost, 4),
    }
    _step_history.append(summary)

    logger.info(
        "COST STEP [%s] complete: %d calls, in=%d out=%d, step=$%.4f | total so far=$%.4f",
        step_name, step_calls, step_in, step_out, step_cost, total_cost,
    )

    _current_step = None
    _step_snapshot = None
    return summary


def estimate_batch_cost(sample_count: int, total_count: int) -> dict:
    """Estimate full batch cost based on sample token usage.

    Call this after a recipe test step to project what the full batch will cost.
    Uses the token usage from the current/last step as the sample baseline.
    """
    if not _step_history:
        return {}
    last = _step_history[-1]
    if sample_count <= 0:
        return {}

    per_product_in = last["input"] / sample_count
    per_product_out = last["output"] / sample_count
    est_in = int(per_product_in * total_count)
    est_out = int(per_product_out * total_count)
    est_cost = _calc_cost(est_in, est_out)
    already_spent = _calc_cost(_token_usage["input"], _token_usage["output"])

    estimate = {
        "sample_count": sample_count,
        "total_count": total_count,
        "per_product_input": int(per_product_in),
        "per_product_output": int(per_product_out),
        "estimated_batch_input": est_in,
        "estimated_batch_output": est_out,
        "estimated_batch_cost": round(est_cost, 4),
        "already_spent": round(already_spent, 4),
        "estimated_total": round(est_cost + already_spent, 4),
    }
    logger.info(
        "COST ESTIMATE: %d products × ~%d in + ~%d out per product = $%.4f batch + $%.4f spent = $%.4f total",
        total_count, int(per_product_in), int(per_product_out),
        est_cost, already_spent, est_cost + already_spent,
    )
    return estimate


def get_token_usage() -> dict:
    """Return cumulative token usage stats with cost breakdown."""
    total_cost = _calc_cost(_token_usage["input"], _token_usage["output"])
    return {
        **_token_usage,
        "cost": round(total_cost, 4),
        "steps": list(_step_history),
    }


def reset_token_usage() -> None:
    """Reset cumulative token counters and step history."""
    _token_usage["input"] = 0
    _token_usage["output"] = 0
    _token_usage["calls"] = 0
    _step_history.clear()


def extract_python_code(text: str) -> str | None:
    """Extract the last Python code block from markdown-formatted LLM response."""
    if not text:
        return None
    # Prefer ```python blocks, fall back to generic ``` blocks
    blocks = re.findall(r"```python\s*\n(.*?)```", text, re.DOTALL)
    if not blocks:
        blocks = re.findall(r"```\s*\n(.*?)```", text, re.DOTALL)
    return blocks[-1].strip() if blocks else None


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
    _log_tokens(response, model_name, "generate_with_text")

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
    _log_tokens(response, model_name, "generate_with_images")

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
    _log_tokens(response, model_name, "generate_with_code_execution")

    return response.text or ""


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
    _log_tokens(response, model_name, "generate_with_search")

    return response.text or ""


def _strip_additional_properties(schema: dict) -> dict:
    """Recursively strip 'additionalProperties' — unsupported by Gemini structured output."""
    if not isinstance(schema, dict):
        return schema
    cleaned = {}
    for k, v in schema.items():
        if k == "additionalProperties":
            continue
        if isinstance(v, dict):
            cleaned[k] = _strip_additional_properties(v)
        elif isinstance(v, list):
            cleaned[k] = [_strip_additional_properties(i) if isinstance(i, dict) else i for i in v]
        else:
            cleaned[k] = v
    return cleaned


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

    # Gemini does not support additionalProperties — strip before sending
    if schema:
        schema = _strip_additional_properties(schema)

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
    _log_tokens(response, model_name, "generate_structured")

    return json.loads(response.text) if response.text else {}
