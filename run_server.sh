#!/bin/bash

# Base dos Dados MCP Server - Claude Desktop Integration Script
# This script provides a reliable wrapper for running the MCP server with Claude Desktop

set -euo pipefail

# Get the directory where this script is located
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

# Ensure we're in the project directory
cd "$SCRIPT_DIR"

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

# Set development environment variables for local testing
export ENVIRONMENT="development"
export LOG_LEVEL="INFO"
export PYTHONPATH="$SCRIPT_DIR/src"

# Log startup for debugging (to stderr so it doesn't interfere with MCP protocol)
echo "Starting Base dos Dados MCP Server..." >&2
echo "Working directory: $SCRIPT_DIR" >&2
echo "Environment: $ENVIRONMENT" >&2
echo "Log level: $LOG_LEVEL" >&2

# Run the MCP server using the development entry point
exec uv run basedosdados-mcp-dev