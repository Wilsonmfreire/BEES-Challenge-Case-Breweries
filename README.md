# BEES-Challenge-Case-Breweries

### Objective:
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

## Project structure
```plaintext
breweries_data
├─── data_injection/
│    ├── notebook/
│    │   ├── bronze_layer.ipynb
│    │   ├── silver_layer.ipynb
│    │   ├── gold_layer.ipynb
│    │   ├── config.json        # Centralized configuration file
│    │   ├── spark_utils.py     # Spark utility functions (if used by scripts)
│    │   ├── utils.py           # Centralized utility functions
│    │   └── README.md
│    └── script/
│        ├── utils.py
│        ├── config.json
│        ├── ing_bronze.ipynb
│        ├── ing_silver.ipynb
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
└── application.txt
```

# Environment Configuration
* pandas
* pyspark
* python 13.12.7
This project requires the following environment variables:

*   `HADOOP_HOME`: Path to Hadoop Installation (ex: `C:/hadoop` on Windows or `/usr/local/hadoop` on Linux/macOS).
*   `SPARK_HOME`: Path to Spark installation (ex: `C:/spark` or `/usr/local/spark`).

**Steps for configuration:**

1.  Install Hadoop and Spark on your machine.
2.  Set the `HADOOP_HOME` and `SPARK_HOME` environment variables pointing to the installation directories.
    *   **Windows:** Use the system's "Environment Variables" settings.
    *   **Linux/macOS:** Add the variables to the `~/.bashrc` or `~/.zshrc` file (ex: `export HADOOP_HOME=/usr/local/hadoop`).
3.  Restart the terminal or computer for the changes to the environment variables to take effect.
