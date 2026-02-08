# ListingAgent

AI agent that helps marketplace sellers create product listings at scale. Upload your product data, have a conversation about your brand, and let the AI generate consistent, high-quality listings across your entire catalog.

Built with Google Gemini 3 for the [Gemini 3 Hackathon](https://gemini3.devpost.com/).

## How It Works

ListingAgent guides you through four phases:

### 1. Upload & Discover
Upload spreadsheets, CSVs, images, or paste raw text. The AI analyzes your data structure, writes and runs extraction scripts in a sandboxed environment, matches images to products, and builds a structured data model. You review and confirm before moving on.

### 2. Brand Interview
A short AI-driven conversation (3-5 exchanges) captures your selling platform, target buyer, brand voice, pricing approach, and listing preferences. The AI shows what it already inferred from your data so you just confirm or correct. Produces a style profile that drives all downstream generation.

### 3. Recipe Building & Testing
The AI drafts a listing "recipe" — a prompt template, output schema, and validation rules — then tests it on sample products. A hybrid evaluation system scores each test listing:

- **Code-based checks** (instant, free): title length, tag count, price presence, mandatory mentions
- **LLM judge** (5 criteria in parallel): brand voice match, description completeness, tag relevance, persuasiveness, data consistency

The recipe auto-refines in a loop, streaming progress via SSE, until quality targets are met or you approve as-is.

### 4. Batch Execution
Applies the finalized recipe to every product with real-time WebSocket progress. Generates complete listings with titles, descriptions, tags, and pricing. Download results as a ZIP.

## Quick Start

```bash
git clone <repo-url>
cd listingagent

# Install dependencies
uv venv && uv pip install -r requirements.txt

# Configure
cp .env.example .env
# Add your GEMINI_API_KEY from https://aistudio.google.com

# Run
.venv/bin/uvicorn main:app --reload --port 8080
```

Open http://localhost:8080.

## Environment Variables

| Variable | Required | Default | Description |
|----------|----------|---------|-------------|
| `GEMINI_API_KEY` | Yes | - | Google AI Studio API key |
| `APP_PASSWORD` | No | `listingagent` | Web UI login password |
| `USE_PRO` | No | `true` | Use Pro model for reasoning tasks (`false` = Flash only) |
| `REASONING_MODEL` | No | `gemini-3-pro-preview` | Model for discovery, interview, recipe building |
| `BATCH_MODEL` | No | `gemini-3-flash-preview` | Model for batch execution and judging |
| `MAX_BATCH_SIZE` | No | `50` | Max products per batch |
| `MAX_UPLOAD_SIZE_MB` | No | `50` | Max upload size (MB) |

## Architecture

```
Vanilla JS + Tailwind (CDN)    No build step
        |
   REST / SSE / WebSocket
        |
FastAPI (Python 3.12)
   ├── discovery.py       Data extraction with sandboxed scripts
   ├── calibration.py     Brand interview with style profile output
   ├── recipe.py          Recipe draft, test, judge, auto-refine loop
   ├── executor.py        Batch execution with progress streaming
   ├── gemini_client.py   Gemini SDK wrapper (retry, text/image/structured/search)
   ├── models.py          Pydantic schemas
   └── file_utils.py      File categorization, image loading, ZIP export
```

Job state lives on disk at `/tmp/jobs/{job_id}/`. Each phase produces a JSON artifact (`data_model.json`, `style_profile.json`, `recipe.json`) consumed by the next.

## Tech Stack

- **Backend**: Python 3.12, FastAPI, uvicorn
- **Frontend**: Vanilla JS, Tailwind CSS via CDN (no build step)
- **AI**: Google Gemini API (`google-genai` SDK) - Pro for reasoning, Flash for batch
- **Data**: pandas, openpyxl, Pillow
- **Real-time**: SSE for auto-refine progress, WebSockets for batch execution

## Key Design Decisions

- **Sandboxed extraction**: The AI writes Python scripts to parse uploaded data. Scripts run in a restricted `exec()` sandbox with whitelisted builtins - no filesystem access, no imports beyond safe modules.
- **Hybrid evaluation**: Scoring avoids the "everything is 70-80%" trap by using decomposed binary criteria. Each LLM judge criterion does chain-of-thought reasoning before a pass/fail verdict. Score = 100 minus 15 per structural issue, minus 12 per failed judge criterion.
- **Auto-refine loop**: Drafts recipe, tests on 3 diverse sample products, evaluates, feeds failures back to the LLM with full context, re-tests. Up to 4 iterations with descriptive progress streaming.
- **Rate-aware**: Gemini free tier is 60 req/min. 3 test products x 5 judge criteria = 15 calls per test round. Batch execution adds 1s delay between items.
- **No build step**: Single-page app with vanilla JS. Tailwind via CDN. Deploy by copying files.

## Deployment

Runs on a DigitalOcean droplet (512MB) as a systemd service. No Docker - runs Python directly.

```bash
ssh root@<server> "cd /opt/listingagent && git pull && systemctl restart listingagent"
```
