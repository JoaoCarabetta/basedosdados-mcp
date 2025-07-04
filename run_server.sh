#!/bin/bash

# Base dos Dados MCP Server - Claude Desktop Integration Script
# This script provides a reliable wrapper for running the MCP server with Claude Desktop

set -e

# =============================================================================
# UTF-8 Encoding Configuration
# =============================================================================

# Ensure proper UTF-8 encoding
export PYTHONIOENCODING=utf-8
export LC_ALL=en_US.UTF-8
export LANG=en_US.UTF-8

# =============================================================================
# BigQuery Configuration
# =============================================================================

# BigQuery settings - can be configured manually or during installation
export GOOGLE_APPLICATION_CREDENTIALS="${GOOGLE_APPLICATION_CREDENTIALS:-}"
export BIGQUERY_PROJECT_ID="${BIGQUERY_PROJECT_ID:-}"
export BIGQUERY_LOCATION="${BIGQUERY_LOCATION:-US}"

# =============================================================================
# Server Startup
# =============================================================================

# Get the directory where this script is located
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

# Navigate to the actual project directory where .venv is located
PROJECT_DIR="/Users/joaoc/Documents/projects/basedosdados_mcp"
cd "$PROJECT_DIR"

# Check if uv is available
if ! command -v uv &> /dev/null; then
    echo "Error: uv is not installed or not in PATH" >&2
    echo "Please install uv: https://docs.astral.sh/uv/getting-started/installation/" >&2
    exit 1
fi

# Check if the virtual environment exists
if [ ! -d ".venv" ]; then
    echo "Error: Virtual environment not found. Please run 'uv sync' first." >&2
    exit 1
fi

# Ativa o ambiente virtual
source .venv/bin/activate

# Set development environment variables for local testing
export ENVIRONMENT="development"
export LOG_LEVEL="INFO"
export PYTHONPATH="$PROJECT_DIR/src"

# Log configuration status
echo "🚀 Starting Base dos Dados MCP Server..." >&2
echo "📊 BigQuery Configuration:" >&2
echo "   - Credentials: ${GOOGLE_APPLICATION_CREDENTIALS:-Not set}" >&2
echo "   - Project ID: ${BIGQUERY_PROJECT_ID:-Not set}" >&2
echo "   - Location: ${BIGQUERY_LOCATION:-US}" >&2

# Check if BigQuery is configured
if [[ -n "$GOOGLE_APPLICATION_CREDENTIALS" && -n "$BIGQUERY_PROJECT_ID" ]]; then
    echo "✅ BigQuery configured - queries will be available" >&2
else
    echo "⚠️  BigQuery not configured - only metadata tools available" >&2
    echo "   To enable BigQuery, set GOOGLE_APPLICATION_CREDENTIALS and BIGQUERY_PROJECT_ID" >&2
fi

echo "" >&2

# Log startup for debugging (to stderr so it doesn't interfere with MCP protocol)
echo "Starting Base dos Dados MCP Server..." >&2
echo "Working directory: $PROJECT_DIR" >&2
echo "Environment: $ENVIRONMENT" >&2
echo "Log level: $LOG_LEVEL" >&2

# Run the MCP server using the development entry point
exec uv run basedosdados-mcp-dev