%md
# Desafio-BEES-Caso-Cervejarias

## Estrutura do projeto

Objective:
The goal of this test is to assess your skills in consuming data from an API, transforming and
persisting it into a data lake following the medallion architecture with three layers: raw data,
curated data partitioned by location, and an analytical aggregated layer.
Instructions:
1. API: Use the Open Brewery DB API to fetch data. The API has an endpoint for
listing breweries: <https://www.openbrewerydb.org/>
2. Orchestration Tool: Choose the orchestration tool of your preference (Airflow,
Luigi, Mage etc.) to build a data pipeline. We're interested in seeing your ability to
handle scheduling, retries, and error handling in the pipeline.
3. Language: Use the language of your preference for the requests and data
transformation. Please include test cases for your code. Python and PySpark are
preferred but not mandatory.
4. Containerization: If you use Docker or Kubernetes for modularization, you'll earn
extra points.
5. Data Lake Architecture: Your data lake must follow the medallion architecture
having a bronze, silver, and gold layer:
a. Bronze Layer: Persist the raw data from the API in its native format or
any format you find suitable.
b. Silver Layer: Transform the data to a columnar storage format such as
parquet or delta, and partition it by brewery location. Please explain any
other transformations you perform.
c. Gold Layer: Create an aggregated view with the quantity of breweries per
type and location.

```plaintext
dados_cervejarias
├─── data_injection/
│    ├── notebook/
│    │   ├── bronze_layer.ipynb
│    │   ├── silver_layer.ipynb
│    │   ├── gold_layer.ipynb
│    │   ├── config.json        # Arquivo de configuração centralizado
│    │   ├── spark_utils.py     # Funções utilitárias do Spark (se usadas por scripts)
│    │   ├── utils.py           # Funções utilitárias centralizadas
│    │   └── README.md
│    └── script/
│        ├── utils.py
│        ├── config.json
│        ├── ingestao_bronze.ipynb
│        ├── ingestao_silver.ipynb
│        └── README.md
├─── datalake/
│    ├── bronze
│    ├── silver
│    ├── gold
│    └── README.md
├─── pipeline/
│    ├── 
│    └── README.md  
├─── pipelines/
│    ├── bronze_dag.py
│    ├── silver_dag.py
│    ├── gold_dag.py
│    └── README.md 
├── README.md
└── requerimento.txt
```

# Configuração do Ambiente
* pandas
* pyspark
* python 13.12.7
Este projeto requer as seguintes variáveis de ambiente:

*   `HADOOP_HOME`: Caminho para a instalação do Hadoop (ex: `C:/hadoop` no Windows ou `/usr/local/hadoop` no Linux/macOS).
*   `SPARK_HOME`: Caminho para a instalação do Spark (ex: `C:/spark` ou `/usr/local/spark`).

**Passos para configuração:**

1.  Instale o Hadoop e o Spark na sua máquina.
2.  Defina as variáveis de ambiente `HADOOP_HOME` e `SPARK_HOME` apontando para os diretórios de instalação.
    *   **Windows:** Use as configurações de "Variáveis de Ambiente" do sistema.
    *   **Linux/macOS:** Adicione as variáveis ao arquivo `~/.bashrc` ou `~/.zshrc` (ex: `export HADOOP_HOME=/usr/local/hadoop`).
3.  Reinicie o terminal ou o computador para que as alterações nas variáveis de ambiente sejam aplicadas.
