# ListingAgent

AI-powered product listing generator for marketplace sellers. ListingAgent automates the creation of product listings at scale by learning your brand voice and style, then applying it consistently across your entire product catalog.

## How It Works

ListingAgent operates in four collaborative phases:

**1. Upload & Discover**
Upload your product data (spreadsheets, images, CSVs) and let AI analyze the structure, identify fields, and understand your data schema automatically.

**2. Interview**
Have a conversation with the AI to define your brand voice, style preferences, pricing strategy, and unique selling points. The AI learns what makes your listings distinctive.

**3. Recipe Building & Auto-Refine**
The AI drafts a processing "recipe" (prompt template + output schema + validation rules), then tests it on sample products. An auto-refine loop iterates automatically — testing, evaluating with a hybrid scoring system, and improving the recipe until quality targets are met. You approve when satisfied.

**4. Batch Execution**
Apply your finalized recipe to all products at once. The AI generates complete, consistent listings across your entire catalog with real-time progress tracking and downloadable results.

## Architecture

```
Frontend (static/index.html + app.js + Tailwind CDN)
    │
    ├── REST + SSE + WebSocket
    │
FastAPI Backend (Python 3.12)
    ├── discovery.py     — Phase 1: Data structure analysis
    ├── calibration.py   — Phase 2: Brand voice interview
    ├── recipe.py        — Phase 3: Recipe design, testing, and auto-refine
    ├── executor.py      — Phase 4: Batch execution engine
    ├── gemini_client.py — Gemini API wrapper and utilities
    ├── models.py        — Pydantic data models
    └── file_utils.py    — File handling and processing
```

**Job State**: Each job maintains its own filesystem directory (`/tmp/jobs/{job_id}/`) with uploads, processed images, and output artifacts. Each phase produces a JSON artifact for the next phase.

## Quick Start

```bash
# Clone the repository
git clone <repo-url>
cd listingagent

# Install dependencies
uv venv && uv pip install -r requirements.txt

# Configure environment
cp .env.example .env
# Edit .env and add your GEMINI_API_KEY from https://aistudio.google.com

# Run the development server
.venv/bin/uvicorn main:app --reload --port 8080

# Open your browser to http://localhost:8080
```

## Environment Variables

| Variable | Required | Default | Description |
|----------|----------|---------|-------------|
| `GEMINI_API_KEY` | Yes | — | Google AI Studio API key from https://aistudio.google.com |
| `APP_PASSWORD` | No | `listingagent` | Shared login password for the web UI |
| `REASONING_MODEL` | No | `gemini-3-pro-preview` | Model for reasoning-heavy tasks (discovery, interview, recipe building) |
| `BATCH_MODEL` | No | `gemini-3-flash-preview` | Model for batch execution (faster, cost-effective) |
| `MAX_BATCH_SIZE` | No | `50` | Maximum number of products to process in a single batch |
| `MAX_UPLOAD_SIZE_MB` | No | `50` | Maximum file upload size in megabytes |

## Tech Stack

- **Backend**: Python 3.12, FastAPI, uvicorn
- **Frontend**: Vanilla JavaScript + Tailwind CSS (CDN, no build step)
- **AI**: Google Gemini API (via `google-genai` SDK)
- **Data Processing**: pandas, openpyxl, Pillow, python-multipart
- **Real-time**: WebSockets for batch progress, SSE for auto-refine streaming
- **Async**: aiofiles for async file operations

## API Endpoints

| Method | Endpoint | Purpose |
|--------|----------|---------|
| POST | `/api/login` | Authenticate with shared password |
| GET | `/api/auth-check` | Validate session cookie |
| POST | `/api/upload` | Upload and categorize files |
| POST | `/api/load-demo` | Load built-in demo dataset |
| POST | `/api/discover` | Analyze data structure (Phase 1) |
| POST | `/api/build-data-model` | Finalize data model from discovery |
| POST | `/api/chat` | Conduct brand voice interview (Phase 2) |
| POST | `/api/test-recipe` | Test recipe on sample products (Phase 3) |
| POST | `/api/auto-refine` | SSE auto-refine loop: draft, test, iterate (Phase 3) |
| POST | `/api/approve-recipe` | Finalize and lock recipe |
| POST | `/api/execute` | Start batch execution (Phase 4) |
| WS | `/ws/{job_id}` | Real-time batch progress streaming |
| GET | `/api/status/{id}` | Job status and statistics |
| GET | `/api/download/{id}` | Download results as ZIP |

## Key Design Decisions

- **Hybrid Evaluation System**: Quality scoring combines two layers — fast, free code-based checks (title length, tag count, price, mandatory mentions) and an LLM judge (5 parallel Flash calls evaluating brand voice, completeness, tag relevance, persuasiveness, and consistency). Each judge criterion uses chain-of-thought reasoning before a binary pass/fail verdict.
- **Auto-Refine Loop**: The `/api/auto-refine` endpoint streams progress via SSE as it drafts, tests, evaluates, and iterates the recipe automatically until quality targets are met.
- **Thinking Levels**: Uses high-level reasoning (Pro model) for discovery, interview, and recipe building. Switches to low-level reasoning (Flash model) for fast batch execution. Escalates to high on retry failures.
- **Tool Constraints**: `code_execution` and `web_search` cannot be used together in the same API call. Code execution is used during recipe testing; validation during batch execution uses local `exec()` for reliability.
- **Rate Limiting**: Respects Gemini free tier limits (60 req/min) with exponential backoff retry. Recommended demo size: 15-20 products.
- **State Management**: Frontend state machine: UPLOAD → DISCOVER → INTERVIEW → RECIPE_TEST → EXECUTING → RESULTS. Chat UI is shared across Phases 1-3 with an evolving context panel.

## Deployment

Runs on a DigitalOcean droplet as a systemd service.

## Built For

[Gemini 3 Hackathon](https://gemini3.devpost.com/)
