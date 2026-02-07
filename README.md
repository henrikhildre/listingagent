# ListingAgent

AI-powered product listing generator for marketplace sellers. ListingAgent automates the creation of product listings at scale by learning your brand voice and style, then applying it consistently across your entire product catalog.

## How It Works

ListingAgent operates in four collaborative phases:

**1. Upload & Discover**
Upload your product data (spreadsheets, images, CSVs) and let AI analyze the structure, identify fields, and understand your data schema automatically.

**2. Interview**
Have a conversation with the AI to define your brand voice, style preferences, pricing strategy, and unique selling points. The AI learns what makes your listings distinctive.

**3. Recipe Building**
Collaboratively design a processing "recipe" that transforms raw product data into polished listings. Test the recipe on sample products, refine prompts, and iterate until results are perfect.

**4. Batch Execution**
Apply your finalized recipe to all products at once. The AI generates complete, consistent listings across your entire catalog with real-time progress tracking and downloadable results.

## Architecture

```
Frontend (static/index.html + app.js + Tailwind CDN)
    │
    ├── REST + WebSocket
    │
FastAPI Backend (Python 3.12)
    ├── discovery.py     — Phase 1: Data structure analysis
    ├── calibration.py   — Phase 2: Brand voice interview
    ├── recipe.py        — Phase 3: Recipe design and testing
    ├── executor.py      — Phase 4: Batch execution engine
    ├── gemini_client.py — Gemini API wrapper and utilities
    ├── models.py        — Pydantic data models
    └── file_utils.py    — File handling and processing
```

**Job State**: Each job maintains its own filesystem directory (`/tmp/jobs/{job_id}/`) with uploads, processed images, and output artifacts. Each phase produces a JSON artifact for the next phase.

## Quick Start

### Local Development

```bash
# Clone the repository
git clone <repo-url>
cd listingagent

# Install dependencies
pip install -r requirements.txt

# Configure environment
cp .env.example .env
# Edit .env and add your GEMINI_API_KEY from https://aistudio.google.com

# Run the development server
uvicorn main:app --reload --port 8080

# Open your browser to http://localhost:8080
```

### Docker

```bash
# Build the image
docker build -t listingagent .

# Run the container
docker run -p 8080:8080 -e GEMINI_API_KEY=your-key listingagent

# Access at http://localhost:8080
```

### Production Deployment (DigitalOcean)

```bash
# Build and push to Docker Hub
docker build -t yourusername/listingagent .
docker push yourusername/listingagent

# SSH into your droplet
ssh root@your-droplet-ip

# Pull and run
docker pull yourusername/listingagent
docker run -d --restart unless-stopped -p 80:8080 \
  -e GEMINI_API_KEY=your-key \
  --name listingagent yourusername/listingagent

# Optional: Use Caddy for automatic HTTPS
# Configure a Caddyfile and run: caddy run
```

## Environment Variables

| Variable | Required | Default | Description |
|----------|----------|---------|-------------|
| `GEMINI_API_KEY` | Yes | — | Google AI Studio API key from https://aistudio.google.com |
| `REASONING_MODEL` | No | `gemini-3-pro-preview` | Model for reasoning-heavy tasks (discovery, interview, recipe building) |
| `BATCH_MODEL` | No | `gemini-3-flash-preview` | Model for batch execution (faster, cost-effective) |
| `MAX_BATCH_SIZE` | No | `50` | Maximum number of products to process in a single batch |
| `MAX_UPLOAD_SIZE_MB` | No | `50` | Maximum file upload size in megabytes |

## Tech Stack

- **Backend**: Python 3.12, FastAPI, uvicorn
- **Frontend**: Vanilla JavaScript + Tailwind CSS (CDN, no build step)
- **AI**: Google Gemini API (via `google-genai` SDK)
- **Data Processing**: pandas, openpyxl, Pillow, python-multipart
- **Real-time**: WebSockets for progress streaming
- **Async**: aiofiles for async file operations

## API Endpoints

| Method | Endpoint | Purpose |
|--------|----------|---------|
| POST | `/api/upload` | Upload and categorize files |
| POST | `/api/discover` | Analyze data structure (Phase 1) |
| POST | `/api/chat` | Conduct brand voice interview (Phase 2) |
| POST | `/api/test-recipe` | Test recipe on sample products (Phase 3) |
| POST | `/api/approve-recipe` | Finalize and lock recipe |
| POST | `/api/execute` | Start batch execution (Phase 4) |
| WS | `/ws/{job_id}` | Real-time progress streaming |
| GET | `/api/status/{id}` | Job status and statistics |
| GET | `/api/download/{id}` | Download results as ZIP |

## Key Design Decisions

- **Thinking Levels**: Uses high-level reasoning (Pro model) for discovery, interview, and recipe building. Switches to low-level reasoning (Flash model) for fast batch execution. Escalates to high on retry failures.
- **Tool Constraints**: `code_execution` and `web_search` cannot be used together in the same API call. Code execution is used during recipe testing; validation during batch execution uses local `exec()` for reliability.
- **Rate Limiting**: Respects Gemini free tier limits (60 req/min) with 1-second delays between batch items. Recommended demo size: 15-20 products.
- **State Management**: Frontend state machine: UPLOAD → DISCOVER → INTERVIEW → RECIPE_TEST → EXECUTING → RESULTS. Chat UI is shared across Phases 1-3 with an evolving context panel.

## Development

For detailed specifications, architecture decisions, and module-level behavior, see [`plan.md`](./plan.md).

For Claude Code integration notes, see [`CLAUDE.md`](./CLAUDE.md).

## Built For

Gemini API Developer Competition 2025

## License

MIT
