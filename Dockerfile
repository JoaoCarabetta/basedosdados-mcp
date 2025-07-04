FROM python:3.11-slim

WORKDIR /app

# Set environment variables
ENV PYTHONUNBUFFERED=1
ENV PYTHONDONTWRITEBYTECODE=1

# Copy project files
COPY pyproject.toml ./
COPY src/ ./src/

# Install the package directly with pip
RUN pip install --no-cache-dir .

# Set production environment
ENV ENVIRONMENT=production

# Run the MCP server
CMD ["basedosdados-mcp"]