#!/bin/bash

# Base dos Dados MCP Server Launcher
# This wrapper script ensures proper environment setup for Claude Desktop

# Set the working directory to the project directory
cd "/Users/joaoc/Documents/projects/basedosdados_mcp"

# Log startup for debugging
echo "Starting Base dos Dados MCP Server..." >&2
echo "Working directory: $(pwd)" >&2
echo "Python path: $(/opt/homebrew/bin/uv run python -c 'import sys; print(sys.executable)')" >&2

# Run the server with uv
exec /opt/homebrew/bin/uv run python server.py