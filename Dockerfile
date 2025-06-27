FROM python:3.11-slim

WORKDIR /app

# Set environment variables for proper stdio handling
ENV PYTHONUNBUFFERED=1
ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONIOENCODING=utf-8

# Install uv for fast dependency management
COPY --from=ghcr.io/astral-sh/uv:latest /uv /uvx /usr/local/bin/

# Copy source code and dependency files
COPY pyproject.toml README.md ./
COPY src/ ./src/

# Create virtual environment and install all dependencies in one step using uv
RUN uv sync --no-dev --no-cache

# Ensure the entry point script is executable and handles stdio properly
ENTRYPOINT ["uv", "run", "python", "-m", "basedosdados_mcp.main"]