# Desafio-BEES-Caso-Cervejarias
Estrutura do projeto:
    dados_cervejarias
    ├─── data_injection/
    |    ├── notebook/
    |    |   ├── bronze_layer.ipynb
    |    |   ├── silver_layer.ipynb
    |    |   ├── gold_layer.ipynb
    |    |   ├── config.json        # Arquivo de configuração centralizado
    |    |   ├── spark_utils.py     # Funções utilitárias do Spark (se usadas por scripts)
    |    |   ├── utils.py           # Funções utilitárias centralizadas
    |    |   └── README.md
    |    └── script/
    |        ├── utils.py
    |        ├── config.json
    |        ├── ingestao_bronze.ipynb
    |        ├── ingestao_silver.ipynb
    |        └── README.md
    ├─── datalake/
    |    ├── bronze
    |    ├── silver
    |    ├── gold
    |    └── README.md
    ├─── pepiline
    |    ├── 
    |    └── README.md  
    ├─── pepilines
    │    ├── bronze_dag.py
    │    ├── silver_dag.py
    |    ├── gold_dag.py
    |    └── README.md 
    ├── README.md
    └── requerimento.txt

Estou para começar uma nova etapa do desafio que é orquestrar usando o airflow, vou criar na pasta pipeline