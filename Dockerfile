FROM python:3.11-slim

WORKDIR /app

# Set environment variables
ENV PYTHONUNBUFFERED=1
ENV PYTHONDONTWRITEBYTECODE=1

# Copy all necessary files
COPY pyproject.toml README.md ./
COPY src/ ./src/

# Install dependencies and package
RUN pip install --no-cache-dir --upgrade pip && \
    pip install --no-cache-dir -e .

# Set the entry point
CMD ["python", "-m", "basedosdados_mcp.main"]