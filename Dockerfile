FROM python:3.11-slim

# Set working directory
WORKDIR /app

# Set environment variables
ENV PYTHONUNBUFFERED=1
ENV PYTHONDONTWRITEBYTECODE=1

# Install system dependencies
RUN apt-get update && apt-get install -y \
    git \
    && rm -rf /var/lib/apt/lists/*

# Copy project files
COPY pyproject.toml ./
COPY README.md ./
COPY src/ ./src/

# Install Python dependencies
RUN pip install --no-cache-dir --upgrade pip && \
    pip install --no-cache-dir .

# Create non-root user for security
RUN useradd --create-home --shell /bin/bash mcp
USER mcp

# Expose port (if needed for HTTP transport)
EXPOSE 8000

# Set the entry point
ENTRYPOINT ["python", "-m", "basedosdados_mcp.main"]

# Health check
HEALTHCHECK --interval=30s --timeout=10s --start-period=5s --retries=3 \
    CMD python -c "import basedosdados_mcp; print('OK')" || exit 1

# Labels for metadata
LABEL org.opencontainers.image.title="Base dos Dados MCP Server"
LABEL org.opencontainers.image.description="Model Context Protocol server for Base dos Dados, Brazil's open data platform"
LABEL org.opencontainers.image.version="0.1.0"
LABEL org.opencontainers.image.authors="João Costa <joao@example.com>"
LABEL org.opencontainers.image.source="https://github.com/joaoc/basedosdados_mcp"
LABEL org.opencontainers.image.licenses="MIT"