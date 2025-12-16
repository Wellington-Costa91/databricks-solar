"""
Volume to Delta - Script principal (Projeto B)
Lê dados de um volume Unity Catalog e escreve em uma tabela Delta
"""

import os
from pyspark.sql import SparkSession
from pyspark.sql import DataFrame
from pyspark.sql.utils import AnalysisException


def get_spark() -> SparkSession:
    """Obtém ou cria uma sessão Spark."""
    return SparkSession.builder.getOrCreate()


def get_volume_path(catalog: str, schema: str, volume_name: str) -> str:
    """
    Constrói o caminho do volume Unity Catalog.
    
    Args:
        catalog: Nome do catálogo
        schema: Nome do schema
        volume_name: Nome do volume
    
    Returns:
        Caminho completo do volume no formato /Volumes/catalog/schema/volume
    """
    return f"/Volumes/{catalog}/{schema}/{volume_name}"


def get_table_name(catalog: str, schema: str, table_name: str) -> str:
    """
    Constrói o nome completo da tabela no Unity Catalog.
    
    Args:
        catalog: Nome do catálogo
        schema: Nome do schema
        table_name: Nome da tabela
    
    Returns:
        Nome completo da tabela no formato catalog.schema.table
    """
    return f"{catalog}.{schema}.{table_name}"


def read_from_volume(
    spark: SparkSession,
    volume_path: str,
    file_format: str = "csv",
    **read_options
) -> DataFrame:
    """
    Lê dados de um volume Unity Catalog.
    
    Args:
        spark: Sessão Spark
        volume_path: Caminho do volume
        file_format: Formato dos arquivos (csv, json, parquet, delta, avro)
        **read_options: Opções adicionais de leitura
    
    Returns:
        DataFrame com os dados lidos
    """
    # Configurações padrão por formato
    default_options = {
        "csv": {"header": "true", "inferSchema": "true", "sep": ","},
        "json": {"multiLine": "true"},
        "parquet": {},
        "delta": {},
        "avro": {},
    }
    
    # Mescla opções padrão com as fornecidas
    options = {**default_options.get(file_format, {}), **read_options}
    
    # Lê os dados conforme o formato
    reader = spark.read.format(file_format)
    
    for key, value in options.items():
        reader = reader.option(key, value)
    
    # Define o caminho de leitura
    read_path = f"{volume_path}/*" if file_format != "delta" else volume_path
    
    print(f"📖 Lendo dados de: {read_path}")
    print(f"📁 Formato: {file_format}")
    print(f"⚙️  Opções: {options}")
    
    df = reader.load(read_path)
    
    print(f"✅ Leitura concluída! Registros: {df.count()}")
    
    return df


def write_to_delta(
    df: DataFrame,
    table_name: str,
    mode: str = "overwrite",
    partition_by: list = None,
    **write_options
) -> None:
    """
    Escreve um DataFrame em uma tabela Delta.
    
    Args:
        df: DataFrame a ser escrito
        table_name: Nome completo da tabela (catalog.schema.table)
        mode: Modo de escrita (overwrite, append, merge)
        partition_by: Lista de colunas para particionamento
        **write_options: Opções adicionais de escrita
    """
    print(f"💾 Escrevendo dados na tabela: {table_name}")
    print(f"📝 Modo: {mode}")
    
    writer = df.write.format("delta").mode(mode)
    
    # Aplica particionamento se especificado
    if partition_by:
        writer = writer.partitionBy(*partition_by)
        print(f"📊 Particionado por: {partition_by}")
    
    # Aplica opções adicionais
    for key, value in write_options.items():
        writer = writer.option(key, value)
    
    # Salva como tabela Delta
    writer.saveAsTable(table_name)
    
    print(f"✅ Tabela {table_name} criada/atualizada com sucesso!")


def process_volume_to_delta(
    catalog: str,
    schema: str,
    volume_name: str,
    table_name: str,
    file_format: str = "csv",
    write_mode: str = "overwrite",
    partition_by: list = None,
    read_options: dict = None,
    write_options: dict = None,
) -> None:
    """
    Processa dados de um volume e escreve em uma tabela Delta.
    
    Args:
        catalog: Nome do catálogo Unity Catalog
        schema: Nome do schema
        volume_name: Nome do volume de origem
        table_name: Nome da tabela de destino
        file_format: Formato dos arquivos no volume
        write_mode: Modo de escrita na tabela Delta
        partition_by: Colunas para particionamento
        read_options: Opções adicionais de leitura
        write_options: Opções adicionais de escrita
    """
    spark = get_spark()
    
    # Constrói caminhos
    volume_path = get_volume_path(catalog, schema, volume_name)
    full_table_name = get_table_name(catalog, schema, table_name)
    
    print("=" * 60)
    print("🚀 VOLUME TO DELTA B - INICIANDO PROCESSAMENTO")
    print("=" * 60)
    print(f"📂 Catálogo: {catalog}")
    print(f"📁 Schema: {schema}")
    print(f"📥 Volume: {volume_name}")
    print(f"📤 Tabela: {table_name}")
    print("=" * 60)
    
    # Lê os dados do volume
    df = read_from_volume(
        spark=spark,
        volume_path=volume_path,
        file_format=file_format,
        **(read_options or {})
    )
    
    # Mostra schema e amostra
    print("\n📋 Schema dos dados:")
    df.printSchema()
    
    print("\n📊 Amostra dos dados:")
    df.show(5, truncate=False)
    
    # Escreve na tabela Delta
    write_to_delta(
        df=df,
        table_name=full_table_name,
        mode=write_mode,
        partition_by=partition_by,
        **(write_options or {})
    )
    
    print("\n" + "=" * 60)
    print("✅ PROCESSAMENTO CONCLUÍDO COM SUCESSO!")
    print("=" * 60)


def main():
    """Função principal - entry point do job."""
    # Obtém configurações das variáveis de ambiente
    catalog = os.environ.get("CATALOG", "main")
    schema = os.environ.get("SCHEMA", "default")
    volume_name = os.environ.get("VOLUME_NAME", "raw_data_b")
    table_name = os.environ.get("TABLE_NAME", "processed_data_b")
    file_format = os.environ.get("FILE_FORMAT", "csv")
    write_mode = os.environ.get("WRITE_MODE", "overwrite")
    
    # Processa os dados
    process_volume_to_delta(
        catalog=catalog,
        schema=schema,
        volume_name=volume_name,
        table_name=table_name,
        file_format=file_format,
        write_mode=write_mode,
    )


if __name__ == "__main__":
    main()

