"""
Phase 3: Recipe Building & Testing.

The core innovation -- collaboratively builds the processing "engine" with the user.

A recipe consists of three artifacts:
1. prompt_template  -- the exact instruction sent to Gemini per product (with {variables})
2. output_schema    -- JSON schema enforced via structured output
3. validation_code  -- Python validation function as a string, run locally via exec()

Workflow: draft -> test on diverse samples -> refine based on feedback -> approve & lock.
"""

import json
import logging
import random
from datetime import datetime, timezone
from pathlib import Path

from gemini_client import (
    generate_with_text,
    generate_structured,
    REASONING_MODEL,
    BATCH_MODEL,
)
from file_utils import get_job_path, load_image_as_bytes

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Default output schema matching ListingOutput
# ---------------------------------------------------------------------------

DEFAULT_OUTPUT_SCHEMA = {
    "type": "object",
    "properties": {
        "title": {"type": "string", "maxLength": 140},
        "description": {"type": "string"},
        "tags": {"type": "array", "items": {"type": "string"}, "maxItems": 13},
        "category_suggestion": {"type": "string"},
        "suggested_price": {"type": "number"},
        "pricing_rationale": {"type": "string"},
        "seo_keywords": {"type": "array", "items": {"type": "string"}},
        "confidence": {"type": "string", "enum": ["high", "medium", "low"]},
        "notes_for_seller": {"type": "string"},
    },
    "required": ["title", "description", "tags", "suggested_price", "confidence"],
}

# ---------------------------------------------------------------------------
# Default validation code
# ---------------------------------------------------------------------------

DEFAULT_VALIDATION_CODE = '''
def validate_listing(listing, style_profile):
    issues = []

    # Word count check
    word_count = len(listing.get("description", "").split())
    if word_count < 50:
        issues.append(f"Description too short ({word_count} words, minimum 50)")
    if word_count > 300:
        issues.append(f"Description too long ({word_count} words, maximum 300)")

    # Tag count
    tags = listing.get("tags", [])
    if len(tags) < 5:
        issues.append(f"Only {len(tags)} tags, aim for at least 10")

    # Mandatory mentions
    desc_lower = listing.get("description", "").lower()
    for mention in style_profile.get("always_mention", []):
        if mention.lower() not in desc_lower:
            issues.append(f"Missing mandatory mention: '{mention}'")

    # Title length
    title = listing.get("title", "")
    if len(title) > 140:
        issues.append("Title exceeds 140 character limit")
    if len(title) < 10:
        issues.append("Title is too short (less than 10 characters)")

    # Price sanity
    price = listing.get("suggested_price", 0)
    if price <= 0:
        issues.append("Invalid price (must be > 0)")

    score = max(0, 100 - (len(issues) * 15))
    return {
        "passed": len(issues) == 0,
        "score": score,
        "issues": issues,
    }
'''


# ---------------------------------------------------------------------------
# Helper: load / save JSON artifacts
# ---------------------------------------------------------------------------

def _load_json(path: Path) -> dict:
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def _save_json(path: Path, data: dict):
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2, default=str)


def load_data_model(job_id: str) -> dict:
    return _load_json(get_job_path(job_id) / "data_model.json")


def load_style_profile(job_id: str) -> dict:
    return _load_json(get_job_path(job_id) / "style_profile.json")


def load_recipe(job_id: str) -> dict:
    return _load_json(get_job_path(job_id) / "recipe.json")


def save_recipe(job_id: str, recipe: dict):
    _save_json(get_job_path(job_id) / "recipe.json", recipe)


# ---------------------------------------------------------------------------
# 1. draft_recipe
# ---------------------------------------------------------------------------

async def draft_recipe(
    job_id: str,
    style_profile: dict,
    data_model: dict,
) -> dict:
    """
    LLM generates the first draft of prompt_template, output_schema,
    and validation_code. Uses Pro model with high thinking.

    Returns a recipe dict ready for testing.
    """

    # Build a summary of available product fields from the data model
    sample_products = data_model.get("products", [])[:3]
    available_fields = set()
    for p in data_model.get("products", []):
        available_fields.add("product_name")
        available_fields.add("product_id")
        if p.get("sku"):
            available_fields.add("sku")
        if p.get("category"):
            available_fields.add("category")
        if p.get("price"):
            available_fields.add("wholesale_price")
        for key in p.get("metadata", {}):
            available_fields.add(key)
        if p.get("image_files"):
            available_fields.add("product_image")

    fields_list = ", ".join(sorted(available_fields))

    sample_data_str = json.dumps(sample_products, indent=2, default=str)

    prompt = f"""You are building a product listing recipe for a marketplace seller.

## Seller Style Profile
{json.dumps(style_profile, indent=2)}

## Available Product Data Fields
{fields_list}

## Sample Products (first 3)
{sample_data_str}

## Your Task
Create a recipe with THREE artifacts:

### 1. Prompt Template
Write the exact prompt that will be sent to an AI model for EACH product.
Use {{curly_brace_variables}} for product-specific data. Available variables:
- {{style_profile_summary}} -- will be filled with the seller style info
- {{product_name}} -- product name
- {{product_id}} -- product identifier
- {{sku}} -- SKU if available
- {{category}} -- category if available
- {{wholesale_price}} -- price if available
- {{metadata}} -- any additional metadata as key: value pairs
- {{title_format}} -- from style profile
- {{description_structure}} -- from style profile
- {{pricing_strategy}} -- from style profile
- {{platform}} -- target platform
- {{always_mention_list}} -- mandatory mentions
- [The product photo will be attached separately]

The prompt should be detailed, covering title format, description style,
tag strategy, pricing approach, and any platform-specific requirements.
Tailor it specifically to this seller's voice and platform.

### 2. Output Schema
A JSON schema for the structured output. Use this default as a starting
point but customize if needed:
{json.dumps(DEFAULT_OUTPUT_SCHEMA, indent=2)}

### 3. Validation Code
Write a Python function called `validate_listing(listing, style_profile)`
that checks the quality of a generated listing. It should return a dict with:
- "passed": bool (True if all checks pass)
- "score": int (0-100)
- "issues": list of strings describing any problems

Tailor the checks to this seller's specific requirements (word counts,
tag counts, mandatory mentions, price ranges, etc.).

## Response Format
Respond with EXACTLY this JSON structure (no markdown fencing):
{{
    "prompt_template": "the full prompt template string...",
    "output_schema": {{}},
    "validation_code": "def validate_listing(listing, style_profile):\\n    ..."
}}
"""

    logger.info("Drafting recipe for job %s", job_id)

    response_text = await generate_with_text(
        prompt,
        model=REASONING_MODEL,
        thinking_level="high",
    )

    # Parse the LLM response -- it should be JSON
    recipe_data = _parse_recipe_response(response_text)

    recipe = {
        "version": 1,
        "prompt_template": recipe_data.get("prompt_template", _default_prompt_template(style_profile)),
        "output_schema": recipe_data.get("output_schema", DEFAULT_OUTPUT_SCHEMA),
        "validation_code": recipe_data.get("validation_code", DEFAULT_VALIDATION_CODE),
        "test_results": [],
        "approved": False,
    }

    # Save draft to disk
    save_recipe(job_id, recipe)
    logger.info("Recipe v%d drafted for job %s", recipe["version"], job_id)

    return recipe


def _parse_recipe_response(text: str) -> dict:
    """Parse the LLM response, handling markdown fencing and partial JSON."""
    # Strip markdown code fences if present
    cleaned = text.strip()
    if cleaned.startswith("```"):
        # Remove opening fence (```json or ```)
        first_newline = cleaned.index("\n")
        cleaned = cleaned[first_newline + 1:]
    if cleaned.endswith("```"):
        cleaned = cleaned[:-3]
    cleaned = cleaned.strip()

    try:
        return json.loads(cleaned)
    except json.JSONDecodeError:
        logger.warning("Failed to parse recipe response as JSON, using defaults")
        # Try to extract individual fields with a more lenient approach
        result = {}

        # Try to find prompt_template
        if '"prompt_template"' in text:
            try:
                # Find the JSON object containing prompt_template
                start = text.index('"prompt_template"')
                # Walk backwards to find the opening brace
                brace_pos = text.rfind("{", 0, start)
                if brace_pos >= 0:
                    # Try parsing from there
                    candidate = text[brace_pos:]
                    result = json.loads(candidate)
            except (json.JSONDecodeError, ValueError):
                pass

        return result


def _default_prompt_template(style_profile: dict) -> str:
    """Fallback prompt template if LLM output cannot be parsed."""
    platform = style_profile.get("platform", "marketplace")
    return f"""You are creating a product listing for a {platform} seller.

## Seller Style
{{style_profile_summary}}

## This Product
- Name: {{product_name}}
- Category: {{category}}
- Wholesale price: {{wholesale_price}}
- Additional info: {{metadata}}
- [Product photo is attached]

## Your Task
Analyze the product photo and available metadata. Create a listing with:
- Title following format: {{title_format}}
- Description following structure: {{description_structure}}
- Tags mixing broad and specific terms
- Suggested retail price based on {{pricing_strategy}}
- SEO keywords for {{platform}}

## Mandatory Mentions
{{always_mention_list}}

Respond in the exact JSON schema provided."""


# ---------------------------------------------------------------------------
# 2. test_recipe
# ---------------------------------------------------------------------------

async def test_recipe(
    job_id: str,
    recipe: dict,
    sample_product_ids: list[str] | None = None,
) -> list[dict]:
    """
    Test the recipe on 2-3 diverse sample products.

    For each sample:
    1. Fill prompt template with product data
    2. Call Gemini with image + filled prompt (structured output)
    3. Run validation locally with exec()
    4. Return listing + validation report

    Returns list of test result dicts.
    """
    data_model = load_data_model(job_id)
    style_profile = load_style_profile(job_id)
    products = data_model.get("products", [])

    if not products:
        raise ValueError("No products found in data model")

    # Select samples
    if sample_product_ids:
        samples = [p for p in products if p.get("id") in sample_product_ids]
    else:
        samples = select_diverse_samples(products, count=3)

    test_results = []

    for product in samples:
        try:
            result = await _test_single_product(
                job_id, recipe, product, style_profile
            )
            test_results.append(result)
        except Exception as e:
            logger.error(
                "Error testing product %s: %s",
                product.get("id", "unknown"),
                str(e),
            )
            test_results.append({
                "product_id": product.get("id", "unknown"),
                "product_name": product.get("name", "Unknown"),
                "listing": None,
                "validation": {
                    "passed": False,
                    "score": 0,
                    "issues": [f"Error during testing: {str(e)}"],
                },
                "image_filename": (product.get("image_files") or [None])[0],
                "error": str(e),
            })

    # Store test results in the recipe
    recipe["test_results"] = test_results
    save_recipe(job_id, recipe)

    return test_results


async def _test_single_product(
    job_id: str,
    recipe: dict,
    product: dict,
    style_profile: dict,
) -> dict:
    """Test recipe on a single product. Returns a test result dict."""

    # 1. Fill the prompt template
    filled_prompt = fill_template(recipe["prompt_template"], product, style_profile)

    # 2. Load product image(s) if available
    image_parts = []
    job_path = get_job_path(job_id)
    image_files = product.get("image_files", [])

    for img_filename in image_files[:2]:  # Max 2 images per product
        # Check in both uploads/ and images/ directories
        for subdir in ["images", "uploads"]:
            img_path = job_path / subdir / img_filename
            if img_path.exists():
                img_bytes, mime_type = load_image_as_bytes(img_path)
                image_parts.append((img_bytes, mime_type))
                break

    # 3. Call Gemini with structured output
    listing = await generate_structured(
        prompt=filled_prompt,
        image_parts=image_parts if image_parts else None,
        schema=recipe.get("output_schema", DEFAULT_OUTPUT_SCHEMA),
        model=BATCH_MODEL,
        thinking_level="medium",
    )

    # 4. Run validation locally
    validation = run_validation(listing, style_profile, recipe.get("validation_code", ""))

    product_id = product.get("id", "unknown")
    logger.info(
        "Tested product %s: score=%d, passed=%s",
        product_id,
        validation.get("score", 0),
        validation.get("passed", False),
    )

    return {
        "product_id": product_id,
        "product_name": product.get("name", "Unknown"),
        "listing": listing,
        "validation": validation,
        "image_filename": image_files[0] if image_files else None,
    }


def fill_template(template: str, product: dict, style_profile: dict) -> str:
    """
    Fill prompt template with product data and style profile info.

    Uses simple string replacement for {variable} placeholders.
    Missing values are replaced with "N/A".
    """
    # Build style profile summary
    style_summary_parts = []
    if style_profile.get("brand_voice"):
        style_summary_parts.append(f"Voice: {style_profile['brand_voice']}")
    if style_profile.get("seller_type"):
        style_summary_parts.append(f"Seller type: {style_profile['seller_type']}")
    if style_profile.get("target_buyer"):
        style_summary_parts.append(f"Target buyer: {style_profile['target_buyer']}")
    style_summary = ". ".join(style_summary_parts) if style_summary_parts else "No style profile provided"

    # Build metadata string
    metadata = product.get("metadata", {})
    metadata_str = ", ".join(f"{k}: {v}" for k, v in metadata.items()) if metadata else "None"

    # Build always-mention list
    always_mention = style_profile.get("always_mention", [])
    always_mention_str = "\n".join(f"- {item}" for item in always_mention) if always_mention else "None"

    # Replacement mapping
    replacements = {
        "{style_profile_summary}": style_summary,
        "{product_name}": product.get("name") or "N/A",
        "{product_id}": product.get("id") or "N/A",
        "{sku}": product.get("sku") or "N/A",
        "{category}": product.get("category") or "N/A",
        "{wholesale_price}": str(product.get("price") or "N/A"),
        "{metadata}": metadata_str,
        "{title_format}": style_profile.get("title_format", "N/A"),
        "{description_structure}": style_profile.get("description_structure", "N/A"),
        "{pricing_strategy}": style_profile.get("pricing_strategy", "N/A"),
        "{platform}": style_profile.get("platform", "marketplace"),
        "{always_mention_list}": always_mention_str,
        "{avg_description_length}": style_profile.get("avg_description_length", "100-200 words"),
        "{tags_style}": style_profile.get("tags_style", "mix of broad and specific"),
    }

    result = template
    for placeholder, value in replacements.items():
        result = result.replace(placeholder, value)

    return result


# ---------------------------------------------------------------------------
# 3. refine_recipe
# ---------------------------------------------------------------------------

async def refine_recipe(
    job_id: str,
    recipe: dict,
    user_feedback: str,
    test_results: list[dict],
) -> dict:
    """
    LLM updates the recipe based on user feedback and test results.

    Increments version number. Can modify prompt_template, output_schema,
    and/or validation_code.

    Returns updated recipe dict.
    """
    style_profile = load_style_profile(job_id)

    # Summarize test results for the LLM
    results_summary = []
    for tr in test_results:
        entry = {
            "product_id": tr.get("product_id"),
            "product_name": tr.get("product_name"),
            "score": tr.get("validation", {}).get("score", 0),
            "passed": tr.get("validation", {}).get("passed", False),
            "issues": tr.get("validation", {}).get("issues", []),
        }
        # Include a snippet of the generated listing for context
        listing = tr.get("listing")
        if listing:
            entry["title_generated"] = listing.get("title", "")
            entry["description_preview"] = listing.get("description", "")[:200]
            entry["tags_count"] = len(listing.get("tags", []))
            entry["price"] = listing.get("suggested_price")
        results_summary.append(entry)

    prompt = f"""You are refining a product listing recipe based on user feedback.

## Current Recipe

### Prompt Template
{recipe["prompt_template"]}

### Output Schema
{json.dumps(recipe.get("output_schema", DEFAULT_OUTPUT_SCHEMA), indent=2)}

### Validation Code
{recipe.get("validation_code", DEFAULT_VALIDATION_CODE)}

## Style Profile
{json.dumps(style_profile, indent=2)}

## Test Results
{json.dumps(results_summary, indent=2)}

## User Feedback
{user_feedback}

## Your Task
Update the recipe to address the user's feedback. You may modify any of the
three artifacts (prompt_template, output_schema, validation_code).

Think carefully about:
- What specifically the user wants changed
- How the test results relate to the feedback
- Whether the prompt, schema, or validation (or all three) need updating

Respond with EXACTLY this JSON structure (no markdown fencing):
{{
    "prompt_template": "the updated prompt template...",
    "output_schema": {{}},
    "validation_code": "def validate_listing(listing, style_profile):\\n    ...",
    "changes_made": "Brief description of what you changed and why"
}}
"""

    logger.info("Refining recipe v%d for job %s", recipe["version"], job_id)

    response_text = await generate_with_text(
        prompt,
        model=REASONING_MODEL,
        thinking_level="high",
    )

    updated_data = _parse_recipe_response(response_text)

    # Build updated recipe, preserving fields the LLM did not return
    updated_recipe = {
        "version": recipe["version"] + 1,
        "prompt_template": updated_data.get("prompt_template", recipe["prompt_template"]),
        "output_schema": updated_data.get("output_schema", recipe.get("output_schema", DEFAULT_OUTPUT_SCHEMA)),
        "validation_code": updated_data.get("validation_code", recipe.get("validation_code", DEFAULT_VALIDATION_CODE)),
        "test_results": recipe.get("test_results", []),
        "approved": False,
        "changes_made": updated_data.get("changes_made", ""),
    }

    save_recipe(job_id, updated_recipe)
    logger.info(
        "Recipe refined to v%d for job %s: %s",
        updated_recipe["version"],
        job_id,
        updated_recipe.get("changes_made", ""),
    )

    return updated_recipe


# ---------------------------------------------------------------------------
# 4. approve_recipe
# ---------------------------------------------------------------------------

async def approve_recipe(job_id: str, recipe: dict) -> dict:
    """
    Lock the recipe. Sets approved=True and saves to recipe.json.

    Returns the approved recipe dict.
    """
    recipe["approved"] = True
    recipe["approved_at"] = datetime.now(timezone.utc).isoformat()

    save_recipe(job_id, recipe)
    logger.info("Recipe v%d approved for job %s", recipe["version"], job_id)

    return recipe


# ---------------------------------------------------------------------------
# 5. select_diverse_samples
# ---------------------------------------------------------------------------

def select_diverse_samples(products: list[dict], count: int = 3) -> list[dict]:
    """
    Pick diverse products for testing.

    Strategy:
    - One with the most metadata (rich data)
    - One with the least metadata (sparse data)
    - One from a different category if available
    - Fill remaining slots randomly

    Returns a list of product dicts.
    """
    if len(products) <= count:
        return list(products)

    selected = []
    remaining = list(products)

    # 1. Pick the product with the most metadata (richest data)
    richest = max(
        remaining,
        key=lambda p: len(p.get("metadata", {})) + (1 if p.get("name") else 0) + (1 if p.get("category") else 0) + (1 if p.get("price") else 0),
    )
    selected.append(richest)
    remaining.remove(richest)

    if len(selected) >= count:
        return selected

    # 2. Pick the product with the least metadata (sparsest data)
    sparsest = min(
        remaining,
        key=lambda p: len(p.get("metadata", {})) + (1 if p.get("name") else 0) + (1 if p.get("category") else 0) + (1 if p.get("price") else 0),
    )
    selected.append(sparsest)
    remaining.remove(sparsest)

    if len(selected) >= count:
        return selected

    # 3. Try to pick from a different category
    selected_categories = {p.get("category") for p in selected}
    different_category = [
        p for p in remaining
        if p.get("category") and p.get("category") not in selected_categories
    ]

    if different_category:
        pick = random.choice(different_category)
        selected.append(pick)
        remaining.remove(pick)
    elif remaining:
        pick = random.choice(remaining)
        selected.append(pick)
        remaining.remove(pick)

    # 4. Fill any remaining slots randomly
    while len(selected) < count and remaining:
        pick = random.choice(remaining)
        selected.append(pick)
        remaining.remove(pick)

    return selected


# ---------------------------------------------------------------------------
# 6. run_validation
# ---------------------------------------------------------------------------

def _check_code_safety(code: str) -> str | None:
    """Parse code with AST and reject dangerous patterns. Returns error string or None."""
    import ast

    try:
        tree = ast.parse(code)
    except SyntaxError as e:
        return f"Syntax error: {e}"

    for node in ast.walk(tree):
        # Block dunder attribute access (e.g. __class__, __base__, __subclasses__)
        if isinstance(node, ast.Attribute) and node.attr.startswith("__"):
            return f"Blocked: access to '{node.attr}' is not allowed"
        # Block import statements
        if isinstance(node, (ast.Import, ast.ImportFrom)):
            return "Blocked: import statements are not allowed"
        # Block exec/eval/compile calls
        if isinstance(node, ast.Call) and isinstance(node.func, ast.Name):
            if node.func.id in ("exec", "eval", "compile", "open", "__import__"):
                return f"Blocked: call to '{node.func.id}' is not allowed"

    return None


def run_validation(
    listing: dict,
    style_profile: dict,
    validation_code: str,
) -> dict:
    """
    Run validation code locally via exec() with restricted globals.

    The validation_code must define a function `validate_listing(listing, style_profile)`
    that returns {"passed": bool, "score": int, "issues": [...]}.

    Returns the validation result dict. Falls back to a basic check on error.
    """
    if not validation_code or not validation_code.strip():
        return _basic_validation(listing, style_profile)

    # AST safety check — reject dangerous patterns before executing
    safety_error = _check_code_safety(validation_code)
    if safety_error:
        logger.warning("Validation code rejected: %s", safety_error)
        return _basic_validation(listing, style_profile)

    # Restricted globals -- only safe builtins
    safe_builtins = {
        "len": len,
        "str": str,
        "int": int,
        "float": float,
        "bool": bool,
        "list": list,
        "dict": dict,
        "set": set,
        "tuple": tuple,
        "range": range,
        "enumerate": enumerate,
        "zip": zip,
        "map": map,
        "filter": filter,
        "sorted": sorted,
        "min": min,
        "max": max,
        "sum": sum,
        "abs": abs,
        "round": round,
        "any": any,
        "all": all,
        "isinstance": isinstance,
        "print": lambda *a, **kw: None,  # no-op print
        "True": True,
        "False": False,
        "None": None,
    }

    exec_globals = {"__builtins__": safe_builtins}
    exec_locals = {}

    try:
        exec(validation_code, exec_globals, exec_locals)

        validate_fn = exec_locals.get("validate_listing")
        if validate_fn is None:
            logger.warning("validation_code does not define validate_listing()")
            return _basic_validation(listing, style_profile)

        result = validate_fn(listing, style_profile)

        # Ensure result has the expected shape
        if not isinstance(result, dict):
            return _basic_validation(listing, style_profile)

        return {
            "passed": bool(result.get("passed", False)),
            "score": int(result.get("score", 0)),
            "issues": list(result.get("issues", [])),
        }

    except Exception as e:
        logger.error("Validation code execution failed: %s", str(e))
        return {
            "passed": False,
            "score": 0,
            "issues": [f"Validation code error: {str(e)}"],
        }


def _basic_validation(listing: dict, style_profile: dict) -> dict:
    """Fallback validation when custom code is unavailable or broken."""
    issues = []

    title = listing.get("title", "")
    if len(title) > 140:
        issues.append("Title exceeds 140 characters")
    if len(title) < 5:
        issues.append("Title is too short")

    description = listing.get("description", "")
    word_count = len(description.split())
    if word_count < 30:
        issues.append(f"Description too short ({word_count} words)")
    if word_count > 500:
        issues.append(f"Description too long ({word_count} words)")

    tags = listing.get("tags", [])
    if len(tags) < 3:
        issues.append(f"Too few tags ({len(tags)})")

    price = listing.get("suggested_price", 0)
    if not isinstance(price, (int, float)) or price <= 0:
        issues.append("Invalid or missing price")

    # Check mandatory mentions
    desc_lower = description.lower()
    for mention in style_profile.get("always_mention", []):
        if mention.lower() not in desc_lower:
            issues.append(f"Missing mandatory mention: '{mention}'")

    score = max(0, 100 - (len(issues) * 15))
    return {
        "passed": len(issues) == 0,
        "score": score,
        "issues": issues,
    }
