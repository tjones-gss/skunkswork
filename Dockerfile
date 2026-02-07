# ===========================================================================
# Build stage
# ===========================================================================
FROM python:3.12-slim AS builder

WORKDIR /app

# Install system deps for building Python packages
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    libpq-dev \
    && rm -rf /var/lib/apt/lists/*

# Install Python dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir --prefix=/install -r requirements.txt

# ===========================================================================
# Runtime stage
# ===========================================================================
FROM python:3.12-slim

WORKDIR /app

# Install runtime system deps (PostgreSQL client, Playwright deps)
RUN apt-get update && apt-get install -y --no-install-recommends \
    libpq5 \
    curl \
    && rm -rf /var/lib/apt/lists/*

# Copy installed Python packages from builder
COPY --from=builder /install /usr/local

# Install Node.js (for Playwright) and Chromium
RUN apt-get update && apt-get install -y --no-install-recommends \
    nodejs \
    npm \
    && npm install -g playwright \
    && npx playwright install chromium --with-deps \
    && rm -rf /var/lib/apt/lists/* \
    && npm cache clean --force

# Copy application code
COPY . .

# Create data directories
RUN mkdir -p data/raw data/processed data/validated data/.state data/dead_letter

# Health check
HEALTHCHECK --interval=30s --timeout=5s --retries=3 \
    CMD python scripts/healthcheck.py

ENTRYPOINT ["python", "-m", "agents.orchestrator"]
