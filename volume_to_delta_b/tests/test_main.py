"""
Testes unitários para o módulo volume_to_delta (Projeto B)
"""

import pytest
from volume_to_delta.main import (
    get_volume_path,
    get_table_name,
)


class TestVolumePath:
    """Testes para a função get_volume_path"""
    
    def test_get_volume_path_basic(self):
        """Testa a construção básica do caminho do volume"""
        result = get_volume_path("catalog", "schema", "volume")
        assert result == "/Volumes/catalog/schema/volume"
    
    def test_get_volume_path_with_special_names(self):
        """Testa com nomes que contêm underscores"""
        result = get_volume_path("my_catalog", "my_schema", "my_volume")
        assert result == "/Volumes/my_catalog/my_schema/my_volume"


class TestTableName:
    """Testes para a função get_table_name"""
    
    def test_get_table_name_basic(self):
        """Testa a construção básica do nome da tabela"""
        result = get_table_name("catalog", "schema", "table")
        assert result == "catalog.schema.table"
    
    def test_get_table_name_with_special_names(self):
        """Testa com nomes que contêm underscores"""
        result = get_table_name("my_catalog", "my_schema", "my_table")
        assert result == "my_catalog.my_schema.my_table"

