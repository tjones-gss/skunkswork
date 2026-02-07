# ===========================================================================
# Build stage
# ===========================================================================
FROM python:3.12-slim AS builder

WORKDIR /app

# Install system deps for building Python packages
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    libpq-dev \
    curl \
    && rm -rf /var/lib/apt/lists/*

# Install Node.js 20 via nodesource
RUN curl -fsSL https://deb.nodesource.com/setup_20.x | bash - \
    && apt-get install -y --no-install-recommends nodejs \
    && rm -rf /var/lib/apt/lists/*

# Install Python dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir --prefix=/install -r requirements.txt

# Install Node dependencies
COPY package.json package-lock.json ./
RUN npm ci --production

# ===========================================================================
# Runtime stage
# ===========================================================================
FROM python:3.12-slim

WORKDIR /app

# Install runtime system deps (PostgreSQL client, Playwright deps, OCR tools)
RUN apt-get update && apt-get install -y --no-install-recommends \
    libpq5 \
    curl \
    tesseract-ocr \
    poppler-utils \
    # Chromium system dependencies
    libnss3 \
    libnspr4 \
    libatk1.0-0 \
    libatk-bridge2.0-0 \
    libcups2 \
    libdrm2 \
    libdbus-1-3 \
    libxkbcommon0 \
    libatspi2.0-0 \
    libxcomposite1 \
    libxdamage1 \
    libxfixes3 \
    libxrandr2 \
    libgbm1 \
    libpango-1.0-0 \
    libcairo2 \
    libasound2 \
    && rm -rf /var/lib/apt/lists/*

# Copy installed Python packages from builder
COPY --from=builder /install /usr/local

# Copy Node.js binary and node_modules from builder
COPY --from=builder /usr/bin/node /usr/bin/node
COPY --from=builder /app/node_modules ./node_modules

# Install Playwright Chromium browser (call CLI directly — npx not copied)
RUN /usr/bin/node node_modules/playwright/cli.js install chromium

# Copy application code (selective COPY)
COPY agents/ ./agents/
COPY config/ ./config/
COPY contracts/ ./contracts/
COPY db/ ./db/
COPY middleware/ ./middleware/
COPY scripts/ ./scripts/
COPY skills/ ./skills/
COPY state/ ./state/

# Create data and logs directories
RUN mkdir -p data/raw data/processed data/validated data/.state data/dead_letter logs

# Health check
HEALTHCHECK --interval=30s --timeout=5s --retries=3 \
    CMD python scripts/healthcheck.py

ENTRYPOINT ["python", "-m", "agents.orchestrator"]
