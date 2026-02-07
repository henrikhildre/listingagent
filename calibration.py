"""
Phase 2: Interview & Style Profile.

Manages the style interview conversation. The LLM drives a short, efficient
interview to understand the seller's brand voice, target buyer, platform
norms, and listing preferences. Adapts questions based on what's already
known from the data model (Phase 1 output).

Produces style_profile.json and conversation.json in the job directory.
"""

import json
import re
from pathlib import Path

from file_utils import get_job_path
from gemini_client import generate_with_text
from models import StyleProfile


# ---------------------------------------------------------------------------
# System prompt for the interview agent
# ---------------------------------------------------------------------------

INTERVIEW_SYSTEM_PROMPT = """\
You are a marketplace listing expert helping a seller define their brand and \
listing style. You will conduct a SHORT interview (3-5 exchanges maximum).

RULES:
- Be efficient. Never ask a question you can answer from the data summary below.
- Show what you've already inferred BEFORE asking a question so the seller \
  can simply confirm or correct.
- Cover these topics (skip any that are already clear from the data):
  1. Selling platform (Etsy, eBay, Vinted, etc.) — affects title limits, tag \
     counts, and description norms.
  2. Target buyer persona — affects tone and vocabulary.
  3. Brand voice / tone — casual, professional, luxurious, playful, etc.
  4. Pricing approach — only ask if prices are NOT already in the data.
  5. Mandatory mentions — sustainability, shipping info, care instructions, etc.
  6. Description structure — paragraphs vs. bullet points, long vs. short.
- Wrap up proactively as soon as you have enough information. Do NOT drag out \
  the conversation.

WHEN YOU HAVE ENOUGH INFORMATION:
End your message with a JSON block wrapped in ```json ... ``` fences. The JSON \
must match this exact schema:

```json
{
  "platform": "etsy",
  "seller_type": "handmade jewelry maker",
  "target_buyer": "women 25-40 looking for affordable statement pieces",
  "brand_voice": "warm, conversational, empowering",
  "description_structure": "short intro paragraph + bullet point features + closing CTA",
  "avg_description_length": "medium (100-200 words)",
  "pricing_strategy": "competitive with 2x markup from materials",
  "tags_style": "long-tail keywords, mix of specific and broad",
  "title_format": "Brand Name | Product Type - Key Feature - Material",
  "always_mention": ["free shipping over $35", "handmade in Portland"],
  "example_listings": []
}
```

Only output the JSON block when you are confident the profile is complete.
Do NOT output partial JSON. Continue interviewing until you have all fields.
"""


def _build_data_context(data_model: dict) -> str:
    """Summarise the data model so the LLM can adapt its questions."""
    lines = ["DATA SUMMARY FROM PHASE 1:"]

    sources = data_model.get("sources", {})
    if sources:
        lines.append(f"- Sources: {json.dumps(sources, default=str)}")

    products = data_model.get("products", [])
    lines.append(f"- Total products: {len(products)}")

    if products:
        sample = products[0]
        fields = [k for k, v in sample.items() if v not in (None, "", [], {})]
        lines.append(f"- Available fields per product: {', '.join(fields)}")

        # Check if prices exist
        has_prices = any(
            p.get("price") is not None for p in products
        )
        if has_prices:
            prices = [p["price"] for p in products if p.get("price") is not None]
            lines.append(
                f"- Prices already provided: yes (range ${min(prices):.2f} - ${max(prices):.2f})"
            )
        else:
            lines.append("- Prices already provided: no")

        # Check if images exist
        has_images = any(p.get("image_files") for p in products)
        lines.append(f"- Images linked to products: {'yes' if has_images else 'no'}")

        # Categories
        cats = list({p.get("category") for p in products if p.get("category")})
        if cats:
            lines.append(f"- Categories found: {', '.join(cats[:10])}")

    matching = data_model.get("matching_strategy", "")
    if matching:
        lines.append(f"- Image matching strategy used: {matching}")

    unmatched = data_model.get("unmatched_images", [])
    if unmatched:
        lines.append(f"- Unmatched images: {len(unmatched)}")

    return "\n".join(lines)


def _build_conversation_prompt(
    data_context: str,
    conversation_history: list[dict],
    user_message: str | None = None,
) -> str:
    """Assemble the full prompt from system instructions, data context,
    conversation history, and the latest user message."""
    parts = [
        INTERVIEW_SYSTEM_PROMPT,
        "",
        data_context,
        "",
        "CONVERSATION SO FAR:",
    ]

    for msg in conversation_history:
        role = msg.get("role", "user").upper()
        parts.append(f"{role}: {msg['content']}")

    if user_message:
        parts.append(f"USER: {user_message}")

    parts.append("")
    parts.append("ASSISTANT:")

    return "\n".join(parts)


def _extract_style_profile(text: str) -> dict | None:
    """Try to extract a style_profile JSON from the model's response.
    Returns the parsed dict if found, otherwise None."""
    # Look for ```json ... ``` fenced block
    pattern = r"```json\s*(\{.*?\})\s*```"
    match = re.search(pattern, text, re.DOTALL)
    if match:
        try:
            data = json.loads(match.group(1))
            # Validate against StyleProfile schema (lenient: fill defaults)
            profile = StyleProfile(
                platform=data.get("platform", "general"),
                seller_type=data.get("seller_type", ""),
                target_buyer=data.get("target_buyer", ""),
                brand_voice=data.get("brand_voice", ""),
                description_structure=data.get("description_structure", ""),
                avg_description_length=data.get("avg_description_length", "medium"),
                pricing_strategy=data.get("pricing_strategy", "market rate"),
                tags_style=data.get("tags_style", ""),
                title_format=data.get("title_format", ""),
                always_mention=data.get("always_mention", []),
                example_listings=data.get("example_listings", []),
            )
            return profile.model_dump()
        except (json.JSONDecodeError, Exception):
            return None
    return None


def _strip_json_block(text: str) -> str:
    """Remove the JSON fenced block from the visible response so the user
    sees a clean message while we capture the profile separately."""
    return re.sub(r"```json\s*\{.*?\}\s*```", "", text, flags=re.DOTALL).strip()


async def _save_artifact(job_id: str, filename: str, data: object) -> None:
    """Persist a JSON artifact to the job directory."""
    job_path = get_job_path(job_id)
    filepath = job_path / filename
    filepath.write_text(json.dumps(data, indent=2, default=str))


async def _load_artifact(job_id: str, filename: str) -> dict | None:
    """Load a JSON artifact from the job directory if it exists."""
    filepath = get_job_path(job_id) / filename
    if filepath.exists():
        return json.loads(filepath.read_text())
    return None


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------


async def start_interview(job_id: str, data_model: dict) -> str:
    """Begin the style interview with context about uploaded data.

    Args:
        job_id: The active job identifier.
        data_model: The data_model dict produced by Phase 1 (discovery).

    Returns:
        The AI's opening message to kick off the interview.
    """
    data_context = _build_data_context(data_model)

    prompt = _build_conversation_prompt(
        data_context=data_context,
        conversation_history=[],
        user_message=None,
    )

    # Append an explicit opening instruction
    prompt += (
        " Begin the interview. Greet the seller briefly, summarise what you "
        "already know from their data, and ask your first question."
    )

    response_text = await generate_with_text(prompt, thinking_level="high")

    # Save the opening exchange
    conversation = [{"role": "assistant", "content": response_text}]
    await _save_artifact(job_id, "conversation.json", conversation)

    # Persist the data_context for future turns
    await _save_artifact(job_id, "_interview_context.json", {
        "data_context": data_context,
        "data_model_summary": {
            "product_count": len(data_model.get("products", [])),
            "has_prices": any(
                p.get("price") is not None
                for p in data_model.get("products", [])
            ),
        },
    })

    return response_text


async def process_message(
    job_id: str,
    user_message: str,
    conversation_history: list[dict],
) -> dict:
    """Process a user message in the interview conversation.

    Args:
        job_id: The active job identifier.
        user_message: The seller's latest message.
        conversation_history: List of {"role": ..., "content": ...} dicts
            representing the conversation so far (both user and assistant turns).

    Returns:
        {
            "response": str,          # The AI's reply (clean, no raw JSON)
            "phase": "interviewing" | "profile_ready",
            "style_profile": dict | None
        }
    """
    # Load saved interview context for data summary
    ctx = await _load_artifact(job_id, "_interview_context.json")
    data_context = ctx["data_context"] if ctx else "No data context available."

    prompt = _build_conversation_prompt(
        data_context=data_context,
        conversation_history=conversation_history,
        user_message=user_message,
    )

    response_text = await generate_with_text(prompt, thinking_level="high")

    # Check if the model decided the profile is complete
    style_profile = _extract_style_profile(response_text)

    if style_profile:
        phase = "profile_ready"
        clean_response = _strip_json_block(response_text)
        if not clean_response:
            clean_response = (
                "Great, I have everything I need! I've built your style profile. "
                "Let's move on to creating your listing recipe."
            )

        # Persist artifacts
        await _save_artifact(job_id, "style_profile.json", style_profile)
    else:
        phase = "interviewing"
        clean_response = response_text

    # Update conversation history and save
    updated_history = list(conversation_history) + [
        {"role": "user", "content": user_message},
        {"role": "assistant", "content": response_text},
    ]
    await _save_artifact(job_id, "conversation.json", updated_history)

    return {
        "response": clean_response,
        "phase": phase,
        "style_profile": style_profile,
    }
