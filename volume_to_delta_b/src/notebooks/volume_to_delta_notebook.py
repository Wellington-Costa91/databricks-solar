# Databricks notebook source
# MAGIC %md
# MAGIC # Volume to Delta - Notebook (Projeto B)
# MAGIC Este notebook lê dados de um volume Unity Catalog e escreve em uma tabela Delta.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuração dos Parâmetros

# COMMAND ----------

# Widgets para receber parâmetros
dbutils.widgets.text("catalog", "main", "Catálogo")
dbutils.widgets.text("schema", "default", "Schema")
dbutils.widgets.text("volume_name", "raw_data_b", "Volume de Origem")
dbutils.widgets.text("table_name", "processed_data_b", "Tabela de Destino")
dbutils.widgets.dropdown("file_format", "csv", ["csv", "json", "parquet", "delta", "avro"], "Formato dos Arquivos")
dbutils.widgets.dropdown("write_mode", "overwrite", ["overwrite", "append"], "Modo de Escrita")

# COMMAND ----------

# Obtém os valores dos parâmetros
catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")
volume_name = dbutils.widgets.get("volume_name")
table_name = dbutils.widgets.get("table_name")
file_format = dbutils.widgets.get("file_format")
write_mode = dbutils.widgets.get("write_mode")

print(f"📂 Catálogo: {catalog}")
print(f"📁 Schema: {schema}")
print(f"📥 Volume: {volume_name}")
print(f"📤 Tabela: {table_name}")
print(f"📁 Formato: {file_format}")
print(f"📝 Modo: {write_mode}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Construção dos Caminhos

# COMMAND ----------

# Caminho do volume Unity Catalog
volume_path = f"/Volumes/{catalog}/{schema}/{volume_name}"
print(f"📂 Caminho do Volume: {volume_path}")

# Nome completo da tabela
full_table_name = f"{catalog}.{schema}.{table_name}"
print(f"📋 Nome da Tabela: {full_table_name}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Leitura dos Dados do Volume

# COMMAND ----------

# Configurações de leitura por formato
read_configs = {
    "csv": {"header": "true", "inferSchema": "true", "sep": ","},
    "json": {"multiLine": "true"},
    "parquet": {},
    "delta": {},
    "avro": {}
}

# Obtém configurações para o formato especificado
config = read_configs.get(file_format, {})

# Define o caminho de leitura
read_path = f"{volume_path}/*" if file_format != "delta" else volume_path

print(f"📖 Lendo dados de: {read_path}")
print(f"⚙️  Configurações: {config}")

# COMMAND ----------

# Lê os dados
reader = spark.read.format(file_format)
for key, value in config.items():
    reader = reader.option(key, value)

df = reader.load(read_path)

print(f"✅ Leitura concluída!")
print(f"📊 Total de registros: {df.count()}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Visualização dos Dados

# COMMAND ----------

# Mostra o schema
print("📋 Schema dos dados:")
df.printSchema()

# COMMAND ----------

# Mostra amostra dos dados
display(df.limit(10))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Escrita na Tabela Delta

# COMMAND ----------

# Escreve na tabela Delta
print(f"💾 Escrevendo dados na tabela: {full_table_name}")
print(f"📝 Modo: {write_mode}")

df.write \
    .format("delta") \
    .mode(write_mode) \
    .saveAsTable(full_table_name)

print(f"✅ Tabela {full_table_name} criada/atualizada com sucesso!")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Validação

# COMMAND ----------

# Valida a tabela criada
result_df = spark.table(full_table_name)
print(f"📊 Total de registros na tabela: {result_df.count()}")
display(result_df.limit(5))

