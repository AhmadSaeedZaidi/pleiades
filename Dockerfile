# Dockerfile — Pleiades Pipeline Image
# Builds a single self-contained image containing Atlas (lib) and Maia (agents),
# so the whole pipeline can run from one image.
#
# Build Context: repository root (pleiades/)
#   docker build -t pleiades-pipeline:latest .
# Run any agent:
#   docker run --env-file .env pleiades-pipeline:latest python -m maia hunter
#   docker run --env-file .env pleiades-pipeline:latest python -m maia painter
# The default command runs the single application scheduler.

FROM python:3.12-slim

# Prevent Python from writing pyc files and enable unbuffered output
ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

WORKDIR /app

# Install system dependencies
# - libpq-dev: Required for psycopg3 (PostgreSQL adapter)
# - gcc: Required for building Python extensions
# - ffmpeg: Required for Invidious stream merging (video+audio DASH)
# - curl/ca-certificates/unzip: Required to fetch and unpack the Deno runtime
RUN apt-get update && apt-get install -y --no-install-recommends \
    libpq-dev \
    gcc \
    ffmpeg \
    curl \
    ca-certificates \
    unzip \
    && rm -rf /var/lib/apt/lists/*

# Install Deno — required by yt-dlp (>=2026.6.9) to solve YouTube's BotGuard/POT
# challenge via the --js-runtimes flag. Without it, painter/scribe/muralist cannot
# generate the player challenge token and get rate-limited by YouTube's bot check.
# yt-dlp 2026 requires Deno >= 2.3.0.
ARG DENO_VERSION=2.9.2
RUN curl -fsSL https://deno.land/install.sh | sh -s v${DENO_VERSION} \
    && mv /root/.deno/bin/deno /usr/local/bin/deno \
    && deno --version

# Upgrade pip and install build tools
RUN pip install --no-cache-dir --upgrade pip setuptools wheel poetry-core

# Copy and install Atlas (The Brain) - Uses Poetry build backend
# Atlas must be installed first as Maia depends on it
COPY tiered_storage /app/tiered_storage
RUN pip install --no-cache-dir -e /app/tiered_storage
COPY atlas /app/atlas
RUN pip install --no-cache-dir -e '/app/atlas[hf]'

# Copy and install Maia (The Agent)
COPY maia /app/maia
RUN pip install --no-cache-dir -e /app/maia

# Set Python path to include source directories
ENV PYTHONPATH=/app/atlas/src:/app/maia/src

# Health check - verify imports + the yt-dlp/Deno POT path (anti-rate-limit).
RUN python -c "from maia import __version__; print(f'Maia v{__version__} ready')" \
    && deno --version \
    && python -m yt_dlp --version

# Default command: the single cadence source.
CMD ["python", "-m", "maia.orchestrator"]
