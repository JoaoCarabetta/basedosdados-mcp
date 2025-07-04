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

# Add health check for production deployments
HEALTHCHECK --interval=30s --timeout=10s --start-period=5s --retries=3 \
    CMD python -c "import sys; sys.exit(0)"

# Set production environment variables
ENV ENVIRONMENT=production
ENV LOG_LEVEL=INFO
ENV PYTHONPATH=/app/src

# Use the correct module path and production entry point
ENTRYPOINT ["uv", "run", "basedosdados-mcp"]