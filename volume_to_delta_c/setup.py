"""
Setup para build do pacote wheel (Projeto C)
"""

from setuptools import setup, find_packages

setup(
    name="volume_to_delta",
    version="0.1.0",
    description="Job Databricks para leitura de volume e escrita em tabela Delta (Projeto C)",
    author="Solar Team",
    packages=find_packages(where="src"),
    package_dir={"": "src"},
    python_requires=">=3.9",
    install_requires=[
        "pyspark>=3.4.0",
    ],
    entry_points={
        "console_scripts": [
            "volume_to_delta=volume_to_delta.main:main",
        ],
    },
)

