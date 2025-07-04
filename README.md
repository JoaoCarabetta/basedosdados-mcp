# Base dos Dados MCP Server



A Model Context Protocol (MCP) server that provides AI-optimized access to Base dos Dados, Brazil's largest open data platform. Features intelligent search, Portuguese language support, and comprehensive dataset exploration capabilities.

## ✨ Features

- **🔍 Smart Search**: Portuguese language support with accent normalization
- **🤖 AI-Optimized**: Single-call comprehensive data retrieval
- **📊 BigQuery Integration**: Ready-to-use SQL generation with table references  
- **🇧🇷 Brazilian Data Focus**: Specialized for Brazilian public datasets (IBGE, RAIS, TSE, etc.)
- **⚡ Efficient**: Intelligent ranking and acronym prioritization
- **📖 Comprehensive**: Dataset overviews, table details, and usage guidance

## 🚀 Installation

### Option 1: One-Line Installation (Recommended)

Install with a single command:

```bash
# Install everything automatically
  bash -i <(curl -LsSf https://raw.githubusercontent.com/JoaoCarabetta/basedosdados-mcp/refs/heads/main/install.sh)
```

The installer will:
- ✅ Check Python version compatibility (3.10-3.14)
- ✅ Install `uv` package manager (if needed)
- ✅ Download and install the MCP package
- ✅ Configure Claude Desktop automatically
- ✅ Validate the installation

**Installation Location:** `~/.local/share/basedosdados-mcp/`

### Option 2: Via PyPI

Install from [PyPI](https://pypi.org/project/basedosdados-mcp/):

```bash
# Install with pip
pip install basedosdados-mcp

# Or with uv (faster)
uv add basedosdados-mcp
```

**Manual Claude Desktop Configuration:**
```json
{
  "mcpServers": {
    "basedosdados": {
      "command": "basedosdados-mcp"
    }
  }
}
```

### Option 3: Manual Installation (Development)

1. **Clone and install**:
   ```bash
   git clone https://github.com/JoaoCarabetta/basedosdados-mcp
   cd basedosdados-mcp
   uv sync
   ```

2. **Run the server**:
   ```bash
   # Production mode
   uv run basedosdados-mcp
   
   # Development mode (with debug logging)
   uv run basedosdados-mcp-dev
   
   # Using the wrapper script
   ./run_server.sh
   ```

3. **Configure Claude Desktop**:
   ```json
   {
     "mcpServers": {
       "basedosdados": {
         "command": "/path/to/basedosdados-mcp/run_server.sh"
       }
     }
   }
   ```


## 🛠️ Available Tools

### Core Tools

| Tool | Description | Example |
|------|-------------|---------|
| **`search_datasets`** | Smart search with Portuguese accent normalization | `search_datasets(query="população", limit=10)` |
| **`get_dataset_overview`** | Complete dataset view with all tables | `get_dataset_overview(dataset_id="DatasetNode:abc123")` |
| **`get_table_details`** | Comprehensive table info with SQL samples | `get_table_details(table_id="TableNode:xyz789")` |


## 💡 Usage Examples

### Search for Brazilian Demographics Data
```python
# Search with Portuguese (works with/without accents)
search_datasets(query="população brasileira")
search_datasets(query="IBGE demografico")
```

### Explore Dataset Structure
```python
# Get complete overview of a dataset (use dataset ID from search results)
get_dataset_overview(dataset_id="DatasetNode:br_ibge_populacao_id")

# Get detailed table information with sample SQL (use table ID from overview)
get_table_details(table_id="TableNode:municipio_id")
```

## 🧠 Key Features

### Portuguese Language Intelligence
- **Accent Normalization**: `populacao` → `população`, `educacao` → `educação`
- **Acronym Recognition**: IBGE, RAIS, TSE, INEP get priority ranking
- **Smart Fallbacks**: Multiple search strategies for robust results

### AI-Optimized Design
- **Single-Call Efficiency**: Comprehensive information without multiple API calls
- **Structured Responses**: Consistent formatting for LLM consumption
- **Ready-to-Use SQL**: Direct BigQuery paths like `basedosdados.br_ibge_populacao.municipio`
- **Context-Aware Guidance**: Next-step instructions included in responses

### Brazilian Data Expertise
- **Government Sources**: IBGE (Census), TSE (Elections), RAIS (Employment)
- **Education Data**: INEP, higher education, school census
- **Economic Indicators**: Central Bank, ministries, regional data
- **Health & Demographics**: SUS, population statistics, vital records

## 🔧 Local Development & Testing

### Setup
```bash
# Clone and install
git clone https://github.com/JoaoCarabetta/basedosdados-mcp
cd basedosdados-mcp
uv sync
```

### Testing with Claude Desktop

#### 1. **Quick Test** (Recommended)
Use the wrapper script for easy Claude Desktop integration:

```json
// Add to your claude_desktop_config.json
{
  "mcpServers": {
    "basedosdados": {
      "command": "/path/to/basedosdados-mcp/run_server.sh"
    }
  }
}
```

#### 2. **Direct Command** (Alternative)
```json
// Add to your claude_desktop_config.json
{
  "mcpServers": {
    "basedosdados": {
      "command": "uv",
      "args": ["run", "basedosdados-mcp-dev"],
      "cwd": "/path/to/basedosdados-mcp"
    }
  }
}
```

#### 3. **Testing the Server**
```bash
# Test server startup (development mode)
uv run basedosdados-mcp-dev

# Test server startup (production mode)  
uv run basedosdados-mcp

# Test with wrapper script
./run_server.sh
```

### Troubleshooting Claude Desktop Integration

**Common Issues:**
- **Server won't start**: Check that `uv sync` completed successfully
- **Tools not available**: Verify the `cwd` path in your config is correct
- **Connection errors**: Check Claude Desktop logs at `~/Library/Logs/Claude/`

**Debug Logging:**
The wrapper script (`run_server.sh`) includes debug output to stderr. Check Claude Desktop logs for startup messages.

### Manual Testing
```bash
# Test tool functionality directly
uv run python -c "
import asyncio
from src.basedosdados_mcp.server import search_datasets
async def test():
    result = await search_datasets('ibge', limit=2)
    print('✅ Search works!')
    print(result[:200] + '...')
asyncio.run(test())
"
```

### Docker Support
```bash
# Build container
docker build -t basedosdados-mcp .

# Run container
docker run --rm basedosdados-mcp
```

## 📚 About Base dos Dados

[Base dos Dados](https://basedosdados.org) is Brazil's largest open data platform, providing standardized access to Brazilian public datasets through BigQuery. This MCP server bridges the gap between AI assistants and Brazil's rich public data ecosystem.

### Data Coverage
- **Demographics**: Population, census, vital statistics
- **Economics**: GDP, employment, trade, inflation
- **Education**: School performance, enrollment, infrastructure  
- **Politics**: Elections, candidates, campaign finance
- **Health**: Hospital data, epidemiology, healthcare infrastructure
- **Environment**: Climate, deforestation, water resources

## 📄 License

MIT License - see [LICENSE](LICENSE) for details.

## 🤝 Contributing

Contributions welcome! Please see our [contributing guidelines](CONTRIBUTING.md) for details.