# Solar - Multi-Project Databricks Asset Bundles

Este repositório contém múltiplos projetos DABs (Databricks Asset Bundles) independentes para processamento de dados.

## 📁 Estrutura do Repositório

```
Solar/
├── README.md                           # Este arquivo
├── .gitignore
├── projects/
│   ├── volume_to_delta_a/              # Projeto A
│   │   ├── databricks.yml
│   │   ├── resources/
│   │   ├── src/
│   │   └── tests/
│   └── volume_to_delta_b/              # Projeto B
│       ├── databricks.yml
│       ├── resources/
│       ├── src/
│       └── tests/
```

## 🚀 Como Usar

Cada projeto DABs é **independente** e deve ser executado a partir do seu próprio diretório.

### Projeto A

```bash
# Navegar para o projeto A
cd projects/volume_to_delta_a

# Validar o bundle
databricks bundle validate

# Deploy no ambiente dev
databricks bundle deploy --target dev

# Executar o job
databricks bundle run volume_to_delta
```

### Projeto B

```bash
# Navegar para o projeto B
cd projects/volume_to_delta_b

# Validar o bundle
databricks bundle validate

# Deploy no ambiente dev
databricks bundle deploy --target dev

# Executar o job
databricks bundle run volume_to_delta
```

## 🔧 Comandos Úteis

### Deploy de todos os projetos (script)

```bash
# Deploy de todos os projetos em dev
for project in projects/*/; do
    echo "📦 Deploying: $project"
    cd "$project"
    databricks bundle deploy --target dev
    cd ../..
done
```

### Executar projeto específico com parâmetros

```bash
cd projects/volume_to_delta_a
databricks bundle run volume_to_delta \
  --var "catalog=meu_catalogo" \
  --var "schema=meu_schema" \
  --var "volume_name=meus_dados" \
  --var "table_name=minha_tabela"
```

## 📋 Projetos Disponíveis

| Projeto | Descrição | Padrões |
|---------|-----------|---------|
| `volume_to_delta_a` | ETL Volume → Delta | `raw_data_a` → `processed_data_a` |
| `volume_to_delta_b` | ETL Volume → Delta | `raw_data_b` → `processed_data_b` |

## ⚙️ Configuração dos Targets

Todos os projetos possuem 3 targets pré-configurados:

| Target | Modo | Descrição |
|--------|------|-----------|
| `dev` | development | Ambiente de desenvolvimento (padrão) |
| `staging` | development | Ambiente de staging |
| `prod` | production | Ambiente de produção |

### Variáveis Comuns

| Variável | Descrição |
|----------|-----------|
| `catalog` | Catálogo Unity Catalog |
| `schema` | Schema onde a tabela será criada |
| `volume_name` | Nome do volume de origem |
| `table_name` | Nome da tabela Delta de destino |
| `file_format` | Formato dos arquivos (csv, json, parquet) |

## 🔐 Configuração do Databricks CLI

### Opção 1: Configuração interativa

```bash
databricks configure
```

### Opção 2: Variáveis de ambiente

```bash
export DATABRICKS_HOST="https://seu-workspace.databricks.com"
export DATABRICKS_TOKEN="seu-token"
```

### Opção 3: Arquivo de configuração (~/.databrickscfg)

```ini
[DEFAULT]
host = https://seu-workspace.databricks.com
token = seu-token
```

## 🆕 Adicionando Novos Projetos

1. Copie um projeto existente:
```bash
cp -r projects/volume_to_delta_a projects/meu_novo_projeto
```

2. Edite o `databricks.yml`:
   - Altere o `bundle.name`
   - Ajuste as variáveis padrão

3. Personalize os arquivos em `src/` e `resources/`

4. Valide e faça deploy:
```bash
cd projects/meu_novo_projeto
databricks bundle validate
databricks bundle deploy
```

## 📚 Documentação

- [Databricks Asset Bundles](https://docs.databricks.com/en/dev-tools/bundles/index.html)
- [Unity Catalog Volumes](https://docs.databricks.com/en/connect/unity-catalog/volumes.html)
- [Databricks CLI](https://docs.databricks.com/en/dev-tools/cli/index.html)

## 📄 Licença

MIT License
