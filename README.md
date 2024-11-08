# brewery_data_pipeline
Este repositório contém um pipeline de dados para consumir informações sobre cervejarias da API Open Brewery DB, realizar transformações e agregações dos dados e, em seguida, salvar os resultados em diferentes formatos (JSON e Parquet). O pipeline segue a arquitetura de medallion (Bronze, Silver, Gold).

## Estrutura do Pipeline
1. **Consumo de Dados (API)**
   - Consumimos os dados da [Open Brewery DB](https://api.openbrewerydb.org/breweries) usando a biblioteca `requests`.

2. **Transformação de Dados (Camadas)**
   - **Bronze**: Salvamento dos dados brutos em formato JSON.
   - **Silver**: Transformação para um formato eficiente como Parquet, particionado por estado.
   - **Gold**: Agregação dos dados, como contagem de cervejarias por tipo e estado.

3. **Testes**
   - O projeto inclui testes unitários para garantir a integridade de cada etapa do pipeline.

## Como Usar
### Pré-requisitos
Antes de rodar o pipeline, você precisa ter as seguintes bibliotecas instaladas:
  - **Python 3.x**
  - **PySpark**
  - **Requests**

Executando o Pipeline
1.	Clone este repositório para o seu ambiente local:
git clone https://github.com/seu-usuario/brewery-data-pipeline.git
cd brewery-data-pipeline

2.	Execute o script principal:
python brewery_data_pipeline.py
Isso irá consumir os dados da API, salvar os dados brutos, transformá-los em Parquet e gerar os dados agregados.

Testes
Os testes unitários deverão ser executados automaticamente ao rodar o script. Você pode também rodar os testes manualmente com:
python -m unittest brewery_data_pipeline.py

Arquitetura de Dados
O pipeline segue a arquitetura de Medalhão e foram organizados da seguinte forma:
  •	Bronze: Dados brutos em formato JSON.
  •	Silver: Dados transformados e salvos como Parquet, particionados por estado.
  •	Gold: Dados agregados com contagem de cervejarias por tipo e estado.
