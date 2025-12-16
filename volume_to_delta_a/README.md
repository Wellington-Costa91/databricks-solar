# Volume to Delta A - Databricks Asset Bundle

Projeto DABs que lê dados de um Volume Unity Catalog e escreve em uma tabela Delta.

## 🚀 Uso Rápido

```bash
# Navegar para o diretório do projeto
cd projects/volume_to_delta_a

# Validar
databricks bundle validate

# Deploy
databricks bundle deploy --target dev

# Executar
databricks bundle run volume_to_delta
```

## ⚙️ Variáveis

| Variável | Padrão |
|----------|--------|
| `catalog` | `main` |
| `schema` | `default` |
| `volume_name` | `raw_data_a` |
| `table_name` | `processed_data_a` |
| `file_format` | `csv` |

Consulte o README principal do projeto Solar para mais detalhes.

