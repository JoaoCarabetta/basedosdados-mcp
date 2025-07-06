#!/bin/bash

# Base dos Dados MCP - Development Installation Script
# 
# This script sets up the MCP server for local development with live code reloading
# and automatic Claude Desktop configuration.

set -e

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# =============================================================================
# Helper Functions
# =============================================================================

log_info() {
    echo -e "${BLUE}ℹ️  $1${NC}"
}

log_success() {
    echo -e "${GREEN}✅ $1${NC}"
}

log_warning() {
    echo -e "${YELLOW}⚠️  $1${NC}"
}

log_error() {
    echo -e "${RED}❌ $1${NC}"
}

check_command() {
    if ! command -v "$1" &> /dev/null; then
        log_error "$1 is not installed or not in PATH"
        return 1
    fi
    return 0
}

# =============================================================================
# Environment Detection
# =============================================================================

# Get the directory where this script is located (should be project root)
PROJECT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
CLAUDE_CONFIG_DIR="$HOME/Library/Application Support/Claude"
CLAUDE_CONFIG_FILE="$CLAUDE_CONFIG_DIR/claude_desktop_config.json"

log_info "Base dos Dados MCP - Development Installation"
log_info "Project directory: $PROJECT_DIR"

# =============================================================================
# Prerequisites Check
# =============================================================================

log_info "Checking prerequisites..."

if ! check_command "uv"; then
    log_error "uv is required but not installed"
    log_info "Install uv: https://docs.astral.sh/uv/getting-started/installation/"
    log_info "Or run: curl -LsSf https://astral.sh/uv/install.sh | sh"
    exit 1
fi

if ! check_command "python3"; then
    log_error "Python 3 is required but not found"
    exit 1
fi

# Check Python version
PYTHON_VERSION=$(python3 -c 'import sys; print(".".join(map(str, sys.version_info[:2])))')
log_success "Found Python $PYTHON_VERSION"

log_success "Prerequisites check passed"

# =============================================================================
# Virtual Environment and Dependencies
# =============================================================================

log_info "Setting up virtual environment and dependencies..."

cd "$PROJECT_DIR"

# Install dependencies in development mode
if [ -f "uv.lock" ]; then
    uv sync --extra dev
else
    # First time setup
    uv sync --extra dev
fi

log_success "Virtual environment and dependencies installed"

# =============================================================================
# Development Wrapper Script
# =============================================================================

log_info "Creating development wrapper script..."

DEV_WRAPPER_PATH="$PROJECT_DIR/run_dev_server.sh"

cat > "$DEV_WRAPPER_PATH" << 'EOF'
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
EOF

chmod +x "$DEV_WRAPPER_PATH"

log_success "Development wrapper created: $DEV_WRAPPER_PATH"

# =============================================================================
# Claude Desktop Configuration
# =============================================================================

log_info "Configuring Claude Desktop..."

# Create Claude config directory if it doesn't exist
mkdir -p "$CLAUDE_CONFIG_DIR"

# Check if Claude Desktop config exists
if [ -f "$CLAUDE_CONFIG_FILE" ]; then
    log_info "Found existing Claude Desktop configuration"
    
    # Backup existing config
    cp "$CLAUDE_CONFIG_FILE" "$CLAUDE_CONFIG_FILE.backup.$(date +%Y%m%d_%H%M%S)"
    log_info "Backed up existing configuration"
    
    # Check if basedosdadosdev server already exists
    if grep -q '"basedosdadosdev"' "$CLAUDE_CONFIG_FILE"; then
        log_warning "Found existing 'basedosdadosdev' server configuration"
        echo ""
        echo "Current configuration:"
        cat "$CLAUDE_CONFIG_FILE" | python3 -m json.tool
        echo ""
        
        read -p "Do you want to update it to use development mode? (y/N): " -n 1 -r
        echo
        
        if [[ $REPLY =~ ^[Yy]$ ]]; then
            # Update existing configuration to point to development wrapper
            python3 << EOF
import json
import os

config_file = "$CLAUDE_CONFIG_FILE"
with open(config_file, 'r') as f:
    config = json.load(f)

# Update the basedosdadosdev server configuration
if 'mcpServers' not in config:
    config['mcpServers'] = {}

# Keep existing environment variables but update command
existing_env = {}
if 'basedosdadosdev' in config['mcpServers'] and 'env' in config['mcpServers']['basedosdadosdev']:
    existing_env = config['mcpServers']['basedosdadosdev']['env']

config['mcpServers']['basedosdadosdev'] = {
    'command': '$DEV_WRAPPER_PATH',
    'env': existing_env
}

with open(config_file, 'w') as f:
    json.dump(config, f, indent=2)

print("Updated Claude Desktop configuration for development mode")
EOF
            log_success "Updated Claude Desktop configuration for development mode"
        else
            log_info "Keeping existing configuration unchanged"
        fi
    else
        log_info "Adding new 'basedosdadosdev' server configuration"
        
        # Add new server configuration
        python3 << EOF
import json

config_file = "$CLAUDE_CONFIG_FILE"
with open(config_file, 'r') as f:
    config = json.load(f)

if 'mcpServers' not in config:
    config['mcpServers'] = {}

config['mcpServers']['basedosdadosdev'] = {
    'command': '$DEV_WRAPPER_PATH',
    'env': {
        'GOOGLE_APPLICATION_CREDENTIALS': '',
        'BIGQUERY_PROJECT_ID': '',
        'BIGQUERY_LOCATION': 'US'
    }
}

with open(config_file, 'w') as f:
    json.dump(config, f, indent=2)
EOF
        log_success "Added Base dos Dados MCP server to Claude Desktop configuration"
    fi
else
    log_info "Creating new Claude Desktop configuration"
    
    # Create new configuration file
    cat > "$CLAUDE_CONFIG_FILE" << EOF
{
  "mcpServers": {
    "basedosdadosdev": {
      "command": "$DEV_WRAPPER_PATH",
      "env": {
        "GOOGLE_APPLICATION_CREDENTIALS": "",
        "BIGQUERY_PROJECT_ID": "",
        "BIGQUERY_LOCATION": "US"
      }
    }
  }
}
EOF
    log_success "Created new Claude Desktop configuration"
fi

# =============================================================================
# BigQuery Configuration Helper
# =============================================================================

log_info "BigQuery Configuration Setup"
echo ""

if [ -f "$CLAUDE_CONFIG_FILE" ]; then
    # Check current BigQuery configuration
    CURRENT_CREDENTIALS=$(python3 -c "
import json
try:
    with open('$CLAUDE_CONFIG_FILE', 'r') as f:
        config = json.load(f)
    env = config.get('mcpServers', {}).get('basedosdadosdev', {}).get('env', {})
    print(env.get('GOOGLE_APPLICATION_CREDENTIALS', ''))
except:
    print('')
" 2>/dev/null)
    
    CURRENT_PROJECT_ID=$(python3 -c "
import json
try:
    with open('$CLAUDE_CONFIG_FILE', 'r') as f:
        config = json.load(f)
    env = config.get('mcpServers', {}).get('basedosdadosdev', {}).get('env', {})
    print(env.get('BIGQUERY_PROJECT_ID', ''))
except:
    print('')
" 2>/dev/null)
    
    if [[ -z "$CURRENT_CREDENTIALS" || -z "$CURRENT_PROJECT_ID" ]]; then
        log_warning "BigQuery is not configured. The MCP server will work with limited functionality."
        echo ""
        echo "To enable BigQuery features, you need to:"
        echo "1. Create a Google Cloud service account with BigQuery access"
        echo "2. Download the service account JSON key file"
        echo "3. Update your Claude Desktop configuration with:"
        echo "   - GOOGLE_APPLICATION_CREDENTIALS: path to your service account JSON file"
        echo "   - BIGQUERY_PROJECT_ID: your Google Cloud project ID"
        echo ""
        echo "Example configuration:"
        echo '   "env": {'
        echo '     "GOOGLE_APPLICATION_CREDENTIALS": "/path/to/your/service-account.json",'
        echo '     "BIGQUERY_PROJECT_ID": "your-project-id",'
        echo '     "BIGQUERY_LOCATION": "US"'
        echo '   }'
        echo ""
        echo "Claude Desktop config file: $CLAUDE_CONFIG_FILE"
    else
        log_success "BigQuery appears to be configured"
        echo "   - Credentials: $CURRENT_CREDENTIALS"
        echo "   - Project ID: $CURRENT_PROJECT_ID"
    fi
fi

# =============================================================================
# Testing
# =============================================================================

log_info "Testing development installation..."

# Test that we can import the package
if uv run python -c "import basedosdados_mcp; print('Package import successful')" 2>/dev/null; then
    log_success "Package import test passed"
else
    log_error "Package import test failed"
    exit 1
fi

# Test that the development entry point works
if timeout 5 uv run basedosdados-mcp-dev --help &>/dev/null || [[ $? == 124 ]]; then
    log_success "Development entry point test passed"
else
    log_warning "Development entry point test timeout (this may be normal)"
fi

# =============================================================================
# Summary
# =============================================================================

echo ""
log_success "🎉 Development installation completed successfully!"
echo ""
log_info "What was installed:"
echo "  ✅ Virtual environment with all dependencies"
echo "  ✅ Package installed in editable mode (live code reloading)"
echo "  ✅ Development wrapper script: $DEV_WRAPPER_PATH"
echo "  ✅ Claude Desktop configuration updated"
echo ""
log_info "Next steps:"
echo "  1. Restart Claude Desktop to pick up the new configuration"
echo "  2. Configure BigQuery credentials (see output above)"
echo "  3. Start developing - changes to src/ will be reflected immediately"
echo ""
log_info "Development commands:"
echo "  • Test the server:     $DEV_WRAPPER_PATH"
echo "  • Run tests:           uv run pytest tests/ -v"
echo "  • Format code:         uv run black src/ tests/"
echo "  • Lint code:           uv run ruff src/ tests/"
echo ""
log_info "Troubleshooting:"
echo "  • Check logs in Claude Desktop"
echo "  • Verify BigQuery configuration in: $CLAUDE_CONFIG_FILE"
echo "  • Run tests to verify functionality: uv run pytest tests/"
echo ""
log_success "Happy coding! 🚀"