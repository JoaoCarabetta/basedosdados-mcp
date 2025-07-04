# 📦 Guia de Publicação - Base dos Dados MCP

Este guia detalha como publicar o MCP server Base dos Dados em diferentes plataformas.

## 🚀 Publicação Automática (Recomendado)

### 1. Criar Release no GitHub

```bash
# 1. Atualizar versão no pyproject.toml
# 2. Commit das alterações
git add .
git commit -m "Release v0.1.0"

# 3. Criar e push da tag
git tag v0.1.0
git push origin main
git push origin v0.1.0
```

O workflow automático irá:
- ✅ Fazer build do pacote
- ✅ Criar release no GitHub
- ✅ Publicar no PyPI
- ✅ Publicar Docker image

### 2. Secrets Necessários no GitHub

Configure estes secrets em `Settings > Secrets and variables > Actions`:

- `PYPI_TOKEN`: Token da API do PyPI
- `DOCKER_USERNAME`: Username do Docker Hub
- `DOCKER_PASSWORD`: Senha/Token do Docker Hub

## 📋 Publicação Manual

### PyPI

```bash
# 1. Build
uv build

# 2. Instalar twine
uv tool install twine

# 3. Upload (testpypi primeiro)
uv tool run twine upload --repository testpypi dist/*

# 4. Upload production
uv tool run twine upload dist/*
```

### Docker Hub

```bash
# 1. Build
docker build -t joaoc/basedosdados-mcp:0.1.0 .
docker build -t joaoc/basedosdados-mcp:latest .

# 2. Login
docker login

# 3. Push
docker push joaoc/basedosdados-mcp:0.1.0
docker push joaoc/basedosdados-mcp:latest
```



## 🔍 Verificação Pós-Publicação

### Testar PyPI
```bash
# Instalar em ambiente limpo
pip install basedosdados-mcp

# Testar comando
basedosdados-mcp --help
```

### Testar Docker
```bash
# Pull e test
docker pull joaoc/basedosdados-mcp
docker run --rm joaoc/basedosdados-mcp
```

### Testar Claude Desktop
```json
{
  "mcpServers": {
    "basedosdados": {
      "command": "basedosdados-mcp"
    }
  }
}
```

## 📈 Próximos Passos

1. **Monitoramento**: Configurar alertas para downloads/uso
2. **Documentação**: Manter README atualizado
3. **Versioning**: Seguir semantic versioning
4. **Feedback**: Coletar feedback da comunidade
5. **Melhorias**: Iterar baseado no uso

## 🆘 Troubleshooting

### PyPI Upload Falha
- Verificar se a versão já existe
- Confirmar token de acesso
- Testar no TestPyPI primeiro

### Docker Build Falha
- Verificar Dockerfile
- Testar build local
- Confirmar credenciais Docker Hub

### Smithery Falha
- Verificar smithery.yaml
- Confirmar login com API key
- Checar formato do arquivo