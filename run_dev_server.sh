#!/bin/bash

# Base dos Dados MCP Server - Development Wrapper
# This script runs the MCP server in development mode with live code reloading

set -e

# Get the directory where this script is located (project root)
PROJECT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

# Navigate to project directory
cd "$PROJECT_DIR"

# Check if uv is available
if ! command -v uv &> /dev/null; then
    echo "Error: uv is not installed or not in PATH" >&2
    echo "Please install uv: https://docs.astral.sh/uv/getting-started/installation/" >&2
    exit 1
fi

# Check if virtual environment exists
if [ ! -d ".venv" ]; then
    echo "Error: Virtual environment not found. Run ./dev_install.sh first." >&2
    exit 1
fi

# Set development environment variables
export ENVIRONMENT="development"
export LOG_LEVEL="DEBUG"
export PYTHONPATH="$PROJECT_DIR/src"

# BigQuery configuration (use from environment or claude config)
export GOOGLE_APPLICATION_CREDENTIALS="${GOOGLE_APPLICATION_CREDENTIALS:-}"
export BIGQUERY_PROJECT_ID="${BIGQUERY_PROJECT_ID:-}"
export BIGQUERY_LOCATION="${BIGQUERY_LOCATION:-US}"

# Log startup information to stderr (MCP protocol uses stdout)
echo "🚀 Starting Base dos Dados MCP Server (Development Mode)..." >&2
echo "📂 Project: $PROJECT_DIR" >&2
echo "🐍 Python: $(uv run python --version)" >&2
echo "📊 BigQuery Configuration:" >&2
echo "   - Credentials: ${GOOGLE_APPLICATION_CREDENTIALS:-Not set}" >&2
echo "   - Project ID: ${BIGQUERY_PROJECT_ID:-Not set}" >&2
echo "   - Location: ${BIGQUERY_LOCATION}" >&2

if [[ -n "$GOOGLE_APPLICATION_CREDENTIALS" && -n "$BIGQUERY_PROJECT_ID" ]]; then
    echo "✅ BigQuery configured - all tools available" >&2
else
    echo "⚠️  BigQuery not configured - only metadata tools available" >&2
    echo "   Set GOOGLE_APPLICATION_CREDENTIALS and BIGQUERY_PROJECT_ID in Claude config" >&2
fi

echo "" >&2
echo "🔧 Development mode: Live code reloading enabled" >&2
echo "📝 Logs: DEBUG level enabled" >&2
echo "" >&2

# Run the MCP server using development entry point
exec uv run basedosdados-mcp-dev
