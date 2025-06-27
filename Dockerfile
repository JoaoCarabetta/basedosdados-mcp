FROM python:3.11-slim

WORKDIR /app

# Set environment variables
ENV PYTHONUNBUFFERED=1
ENV PYTHONDONTWRITEBYTECODE=1

# Install uv for fast dependency management
COPY --from=ghcr.io/astral-sh/uv:latest /uv /uvx /usr/local/bin/

# Copy source code and dependency files
COPY pyproject.toml README.md ./
COPY src/ ./src/

# Create virtual environment and install all dependencies in one step using uv
RUN uv sync --no-dev --no-cache

# Set the entry point
CMD ["uv", "run", "python", "-m", "basedosdados_mcp.main"]