#!/bin/bash

# Base dos Dados MCP Server Installer
# This script downloads the MCP package, installs dependencies, and configures Claude Desktop

set -euo pipefail

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Function to print colored output
print_status() {
    echo -e "${BLUE}[INFO]${NC} $1"
}

print_success() {
    echo -e "${GREEN}[SUCCESS]${NC} $1"
}

print_warning() {
    echo -e "${YELLOW}[WARNING]${NC} $1"
}

print_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

# Function to check if a command exists
command_exists() {
    command -v "$1" >/dev/null 2>&1
}

# Function to install uv if not present
install_uv() {
    if ! command_exists uv; then
        print_status "Installing uv package manager..."
        curl -LsSf https://astral.sh/uv/install.sh | sh
        # Add uv to PATH for current session
        export PATH="$HOME/.cargo/bin:$PATH"
        print_success "uv installed successfully"
    else
        print_status "uv is already installed"
    fi
}

# Function to check Python version
check_python_version() {
    if command_exists python3; then
        PYTHON_VERSION=$(python3 -c "import sys; print(f'{sys.version_info.major}.{sys.version_info.minor}')")
        REQUIRED_MIN="3.10"
        REQUIRED_MAX="3.14"
        
        if python3 -c "import sys; exit(0 if '3.10' <= sys.version <= '3.14' else 1)" 2>/dev/null; then
            print_success "Python $PYTHON_VERSION is compatible"
        else
            print_error "Python $PYTHON_VERSION is not compatible. Required: $REQUIRED_MIN - $REQUIRED_MAX"
            exit 1
        fi
    else
        print_error "Python 3 is not installed"
        exit 1
    fi
}

# Function to download and install the MCP package
install_mcp_package() {
    print_status "Downloading and installing Base dos Dados MCP package..."
    
    # Create a temporary directory for the installation
    TEMP_DIR=$(mktemp -d)
    cd "$TEMP_DIR"
    
    print_status "Downloading package from GitHub..."
    
    # Download the latest release or main branch
    if command_exists curl; then
        curl -LsSf -o basedosdados-mcp.tar.gz https://github.com/JoaoCarabetta/basedosdados-mcp/archive/refs/heads/main.tar.gz
    elif command_exists wget; then
        wget -q -O basedosdados-mcp.tar.gz https://github.com/JoaoCarabetta/basedosdados-mcp/archive/refs/heads/main.tar.gz
    else
        print_error "Neither curl nor wget is available. Please install one of them."
        exit 1
    fi
    
    # Extract the archive
    print_status "Extracting package..."
    tar -xzf basedosdados-mcp.tar.gz
    cd basedosdados-mcp-main
    
    # Install dependencies using uv
    print_status "Installing dependencies with uv..."
    uv sync
    
    # Install the package in development mode
    print_status "Installing package in development mode..."
    uv pip install -e .
    
    # Copy the run_server.sh script to a permanent location
    INSTALL_DIR="$HOME/.local/share/basedosdados-mcp"
    mkdir -p "$INSTALL_DIR"
    cp run_server.sh "$INSTALL_DIR/"
    chmod +x "$INSTALL_DIR/run_server.sh"
    
    # Clean up temporary directory
    cd /
    rm -rf "$TEMP_DIR"
    
    print_success "MCP package installed successfully at $INSTALL_DIR"
}

# Function to configure Claude Desktop
configure_claude_desktop() {
    print_status "Configuring Claude Desktop..."
    
    # Get the current user's home directory
    USER_HOME="$HOME"
    
    # Define the config file path
    CONFIG_FILE="$USER_HOME/Library/Application Support/Claude/claude_desktop_config.json"
    
    # Check if the config file exists
    if [ ! -f "$CONFIG_FILE" ]; then
        print_error "Config file not found at $CONFIG_FILE"
        print_warning "Please make sure Claude Desktop is installed and has been run at least once"
        return 1
    fi
    
    # Create backup of the original file
    BACKUP_FILE="$CONFIG_FILE.backup.$(date +%Y%m%d_%H%M%S)"
    cp "$CONFIG_FILE" "$BACKUP_FILE"
    print_status "Backup created at $BACKUP_FILE"
    
    # Get the absolute path to run_server.sh
    SERVER_SCRIPT="$HOME/.local/share/basedosdados-mcp/run_server.sh"
    
    # Check if jq is available for JSON manipulation
    if command_exists jq; then
        print_status "Using jq for JSON manipulation..."
        
        # Check if mcpServers section exists
        if ! jq -e '.mcpServers' "$CONFIG_FILE" >/dev/null 2>&1; then
            print_status "Creating mcpServers section..."
            # Add mcpServers section if it doesn't exist
            jq '. + {"mcpServers": {}}' "$CONFIG_FILE" > "${CONFIG_FILE}.tmp" && mv "${CONFIG_FILE}.tmp" "$CONFIG_FILE"
        fi
        
        # Check if basedosdados configuration already exists in mcpServers
        if jq -e '.mcpServers.basedosdados' "$CONFIG_FILE" >/dev/null 2>&1; then
            print_warning "basedosdados configuration already exists in mcpServers"
            print_status "Updating existing configuration..."
            
            # Update the existing configuration
            jq --arg cmd "$SERVER_SCRIPT" '.mcpServers.basedosdados.command = $cmd' "$CONFIG_FILE" > "${CONFIG_FILE}.tmp" && mv "${CONFIG_FILE}.tmp" "$CONFIG_FILE"
        else
            print_status "Adding new basedosdados configuration to mcpServers..."
            
            # Add the new configuration to mcpServers
            jq --arg cmd "$SERVER_SCRIPT" '.mcpServers.basedosdados = {"command": $cmd}' "$CONFIG_FILE" > "${CONFIG_FILE}.tmp" && mv "${CONFIG_FILE}.tmp" "$CONFIG_FILE"
        fi
        
        # Remove any duplicate basedosdados entry outside of mcpServers
        if jq -e '.basedosdados' "$CONFIG_FILE" >/dev/null 2>&1; then
            print_warning "Removing duplicate basedosdados entry outside mcpServers..."
            jq 'del(.basedosdados)' "$CONFIG_FILE" > "${CONFIG_FILE}.tmp" && mv "${CONFIG_FILE}.tmp" "$CONFIG_FILE"
        fi
        
    else
        print_warning "jq not found, using sed for JSON manipulation (less reliable)"
        
        # Fallback to sed-based manipulation
        # This is more complex and less reliable, but provides a fallback
        if grep -q '"mcpServers"' "$CONFIG_FILE"; then
            if grep -q '"basedosdados"' "$CONFIG_FILE"; then
                print_warning "basedosdados configuration already exists"
            else
                # Insert the basedosdados config inside mcpServers
                sed -i '' 's/"mcpServers": {/"mcpServers": {\n    "basedosdados": {\n      "command": "'"$SERVER_SCRIPT"'"\n    }/' "$CONFIG_FILE"
            fi
        else
            print_error "mcpServers section not found and jq not available for automatic creation"
            print_warning "Please manually add the basedosdados configuration to the mcpServers section"
            return 1
        fi
    fi
    
    print_success "Claude Desktop configured successfully"
    print_status "Configuration file: $CONFIG_FILE"
}

# Function to validate installation
validate_installation() {
    print_status "Validating installation..."
    
    # Check if the server script is executable
    SERVER_SCRIPT="$HOME/.local/share/basedosdados-mcp/run_server.sh"
    
    if [ ! -x "$SERVER_SCRIPT" ]; then
        print_error "Server script is not executable: $SERVER_SCRIPT"
        return 1
    fi
    
    # Test if the MCP server can start (timeout after 5 seconds)
    print_status "Testing MCP server startup..."
    if timeout 5s "$SERVER_SCRIPT" >/dev/null 2>&1; then
        print_success "MCP server test passed"
    else
        print_warning "MCP server test failed (this might be normal if no input is provided)"
    fi
    
    print_success "Installation validation completed"
}

# Function to show installation info
show_installation_info() {
    print_success "Installation completed successfully!"
    echo ""
    print_status "Installation location: $HOME/.local/share/basedosdados-mcp"
    print_status "Server script: $HOME/.local/share/basedosdados-mcp/run_server.sh"
    echo ""
    print_status "You can now use the Base dos Dados MCP server in Claude Desktop"
    print_status "To test the server manually, run: $HOME/.local/share/basedosdados-mcp/run_server.sh"
    echo ""
    print_status "To uninstall, run: rm -rf $HOME/.local/share/basedosdados-mcp"
}

# Main installation function
main() {
    print_status "Starting Base dos Dados MCP Server installation..."
    
    # Check Python version
    check_python_version
    
    # Install uv if needed
    install_uv
    
    # Install the MCP package
    install_mcp_package
    
    # Configure Claude Desktop
    if configure_claude_desktop; then
        print_success "Claude Desktop configuration completed"
    else
        print_warning "Claude Desktop configuration failed - you may need to configure it manually"
        # Don't exit here, continue with BigQuery configuration
    fi
    
    # Validate installation
    validate_installation
    
    # Show installation info
    show_installation_info
    
    # Debug: Check if we're about to start BigQuery configuration
    print_status "About to start BigQuery configuration section"

    echo " Base dos Dados MCP instalado com sucesso!"
    echo ""

    # Debug: Check if we reached this point
    print_status "Reached BigQuery configuration section"
    
    # Pergunta sobre BigQuery
    echo "📊 Você gostaria de ativar a execução de queries no BigQuery?"
    echo "   Isso permite executar SQL diretamente nos dados da Base dos Dados."
    
    # Check if we're in an interactive terminal
    if [ -t 0 ]; then
        read -p "   Ativar BigQuery? (Y/n): " enable_bigquery
        # Default to "Y" if user just presses Enter
        enable_bigquery=${enable_bigquery:-Y}
    else
        print_warning "Non-interactive environment detected. Skipping BigQuery configuration."
        print_warning "To configure BigQuery manually, edit the Claude Desktop config file."
        enable_bigquery="N"
    fi

    if [[ $enable_bigquery =~ ^[Yy]$ ]] || [[ -z "$enable_bigquery" ]]; then
        echo ""
        echo "🔧 Configuração do BigQuery:"
        
        # Solicita project-id
        read -p "   Project ID (ex: rj-escritorio-dev): " project_id
        
        # Solicita location
        read -p "   Location (ex: US, us-central1) [US]: " location
        location=${location:-US}
        
        # Solicita key-file
        read -p "   Caminho para o arquivo de credenciais (ex: /path/to/service-account.json): " key_file
        
        # Valida se o arquivo existe
        if [[ ! -f "$key_file" ]]; then
            echo "❌ Arquivo de credenciais não encontrado: $key_file"
            echo "   Certifique-se de que o arquivo existe e tente novamente."
            exit 1
        fi
        
        # Atualiza o Claude Desktop config
        claude_config="$HOME/Library/Application Support/Claude/claude_desktop_config.json"
        
        if [[ -f "$claude_config" ]]; then
            echo "📝 Atualizando configuração do Claude Desktop..."
            
            # Cria backup do config original
            cp "$claude_config" "${claude_config}.backup"
            
            # Atualiza o config com as variáveis de ambiente do BigQuery
            # Usa jq para modificar o JSON de forma segura
            if command -v jq &> /dev/null; then
                # Se jq está disponível, usa ele para modificar o JSON
                jq --arg cmd "$HOME/.local/share/basedosdados-mcp/run_server.sh" \
                   --arg creds "$key_file" \
                   --arg proj "$project_id" \
                   --arg loc "$location" \
                   '.mcpServers.basedosdados = {
                       "command": $cmd,
                       "env": {
                           "GOOGLE_APPLICATION_CREDENTIALS": $creds,
                           "BIGQUERY_PROJECT_ID": $proj,
                           "BIGQUERY_LOCATION": $loc
                       }
                   }' "$claude_config" > "${claude_config}.tmp" && mv "${claude_config}.tmp" "$claude_config"
                
                echo "✅ Configuração do Claude Desktop atualizada!"
            else
                # Fallback: cria um novo config se jq não estiver disponível
                cat > "$claude_config" << EOF
{
  "mcpServers": {
    "basedosdados": {
      "command": "$HOME/.local/share/basedosdados-mcp/run_server.sh",
      "env": {
        "GOOGLE_APPLICATION_CREDENTIALS": "$key_file",
        "BIGQUERY_PROJECT_ID": "$project_id",
        "BIGQUERY_LOCATION": "$location"
      }
    }
  }
}
EOF
                echo "✅ Configuração do Claude Desktop criada!"
            fi
            
            echo "📁 Backup do config original: ${claude_config}.backup"
            echo ""
            echo "🔄 Reinicie o Claude Desktop para aplicar as mudanças!"
            
        else
            echo "⚠️  Arquivo de configuração do Claude Desktop não encontrado."
            echo "   Crie manualmente em: $claude_config"
            echo ""
            echo "   Exemplo de configuração:"
            echo "   {"
            echo "     \"mcpServers\": {"
            echo "       \"basedosdados\": {"
            echo "         \"command\": \"$HOME/.local/share/basedosdados-mcp/run_server.sh\","
            echo "         \"env\": {"
            echo "           \"GOOGLE_APPLICATION_CREDENTIALS\": \"$key_file\","
            echo "           \"BIGQUERY_PROJECT_ID\": \"$project_id\","
            echo "           \"BIGQUERY_LOCATION\": \"$location\""
            echo "         }"
            echo "       }"
            echo "     }"
            echo "   }"
        fi
        
    else
        echo "ℹ️  BigQuery não foi ativado."
        echo "   Para ativar manualmente, edite o claude_desktop_config.json e adicione:"
        echo "   \"env\": {"
        echo "     \"GOOGLE_APPLICATION_CREDENTIALS\": \"/path/to/service-account.json\","
        echo "     \"BIGQUERY_PROJECT_ID\": \"your-project-id\","
        echo "     \"BIGQUERY_LOCATION\": \"US\""
        echo "   }"
    fi

    echo ""
    echo "🎉 Instalação concluída!"
}

# Parse command line arguments
INSTALL_CLAUDE_DESKTOP=true

while [[ $# -gt 0 ]]; do
    case $1 in
        --no-claude-desktop)
            INSTALL_CLAUDE_DESKTOP=false
            shift
            ;;
        --help|-h)
            echo "Usage: curl -LsSf https://raw.githubusercontent.com/JoaoCarabetta/basedosdados-mcp/main/install.sh | sh [OPTIONS]"
            echo ""
            echo "Options:"
            echo "  --no-claude-desktop    Skip Claude Desktop configuration"
            echo "  --help, -h            Show this help message"
            echo ""
            echo "This script will:"
            echo "  1. Check Python version compatibility"
            echo "  2. Install uv package manager (if needed)"
            echo "  3. Download and install the Base dos Dados MCP package"
            echo "  4. Configure Claude Desktop (unless --no-claude-desktop is used)"
            echo "  5. Validate the installation"
            exit 0
            ;;
        *)
            print_error "Unknown option: $1"
            echo "Use --help for usage information"
            exit 1
            ;;
    esac
done

# Run main installation
main "$@"
