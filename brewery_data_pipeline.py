import requests
import json
import os
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
import unittest


# 1. Consumo da API (Fetch)
def fetch_breweries():
    """Consome dados da API Open Brewery DB."""
    url = "https://api.openbrewerydb.org/breweries"
    try:
        response = requests.get(url)
        response.raise_for_status()  # Levanta exceção se a resposta for diferente de 200
        return response.json()
    except requests.exceptions.RequestException as e:
        print(f"Erro ao acessar a API: {e}")
        return []


# 2. Transformação e Armazenamento (Medallion Architecture)
# Bronze Layer (Camada Bruta)
def save_raw_data(data, filepath):
    """Salva os dados brutos em um arquivo JSON."""
    try:
        with open(filepath, 'w') as f:
            json.dump(data, f, indent=4)
        print(f"Dados brutos salvos em {filepath}")
    except Exception as e:
        print(f"Erro ao salvar dados brutos: {e}")


# Silver Layer (Camada Curada)
def transform_to_parquet(input_filepath, output_filepath):
    """Transforma dados brutos para um formato mais eficiente como Parquet."""
    # Inicializa Spark
    spark = SparkSession.builder.appName('BreweryDataPipeline').getOrCreate()

    try:
        # Carrega dados brutos
        df = spark.read.json(input_filepath)

        # Realiza transformações (filtragem, por exemplo)
        df_transformed = df.select("id", "name", "brewery_type", "city", "state") \
                           .filter(col("state").isNotNull())

        # Salva os dados transformados em formato Parquet, particionado por estado
        df_transformed.write.partitionBy("state").parquet(output_filepath)
        print(f"Dados transformados salvos em {output_filepath}")
    except Exception as e:
        print(f"Erro ao transformar dados para Parquet: {e}")
    finally:
        spark.stop()


# Gold Layer (Camada Agregada)
def aggregate_data(input_filepath, output_filepath):
    """Agrega dados para insights, como número de cervejarias por tipo e localização."""
    # Inicializa Spark
    spark = SparkSession.builder.appName('BreweryDataPipeline').getOrCreate()

    try:
        # Carrega dados transformados em Parquet
        df = spark.read.parquet(input_filepath)

        # Agrega os dados (contagem de cervejarias por tipo e estado)
        aggregated_df = df.groupBy("brewery_type", "state").count()

        # Salva a visão agregada
        aggregated_df.write.parquet(output_filepath)
        print(f"Dados agregados salvos em {output_filepath}")
    except Exception as e:
        print(f"Erro ao agregar dados: {e}")
    finally:
        spark.stop()


# 3. Testes (Unitários)
class TestBreweryPipeline(unittest.TestCase):

    def test_fetch_breweries(self):
        """Testa a função de consumo da API."""
        data = fetch_breweries()
        self.assertGreater(len(data), 0, "A lista de cervejarias não pode ser vazia!")

    def test_save_raw_data(self):
        """Testa o salvamento de dados brutos."""
        data = fetch_breweries()
        save_raw_data(data, 'raw_breweries.json')
        with open('raw_breweries.json', 'r') as f:
            saved_data = json.load(f)
        self.assertEqual(len(data), len(saved_data), "Os dados salvos não correspondem aos dados originais!")

    def test_transform_to_parquet(self):
        """Testa a transformação dos dados para Parquet."""
        save_raw_data(fetch_breweries(), 'raw_breweries.json')
        transform_to_parquet('raw_breweries.json', 'silver_breweries.parquet')
        # Verifique se o arquivo Parquet foi gerado
        self.assertTrue(os.path.exists('silver_breweries.parquet'), "Arquivo Parquet não foi gerado!")

    def test_aggregate_data(self):
        """Testa a agregação dos dados para insights."""
        aggregate_data('silver_breweries.parquet', 'gold_breweries_aggregated.parquet')
        # Verifique se o arquivo Parquet de dados agregados foi gerado
        self.assertTrue(os.path.exists('gold_breweries_aggregated.parquet'), "Arquivo Parquet agregados não foi gerado!")


# 4. Função Principal do Script
def main():
    """Função principal para orquestrar o pipeline."""
    print("Iniciando o pipeline de dados de cervejarias...")

    # Passo 1: Consumir dados da API
    breweries_data = fetch_breweries()

    if breweries_data:
        # Passo 2: Salvar dados brutos (Camada Bronze)
        save_raw_data(breweries_data, 'raw_breweries.json')

        # Passo 3: Transformar para Parquet (Camada Silver)
        transform_to_parquet('raw_breweries.json', 'silver_breweries.parquet')

        # Passo 4: Agregar os dados (Camada Gold)
        aggregate_data('silver_breweries.parquet', 'gold_breweries_aggregated.parquet')

    else:
        print("Nenhum dado foi retornado pela API.")


# Rodar o pipeline se o script for executado diretamente
if __name__ == "__main__":
    main()

    # Executar os testes (caso queira testar o código)
    unittest.main(argv=[''], verbosity=2, exit=False)
