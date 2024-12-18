from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.utils import AnalysisException
from spark_utils import iniciar_spark, fechar_spark
from utils import ler_configuracoes, get_latest_folder_with_file

import os

spark = iniciar_spark("ingestaoSilver")

# Variaveis
layer_bronze = "bronze"
bronze_path = ler_configuracoes(layer_bronze)
layer_silver = "silver"
silver_path = ler_configuracoes(layer_silver)

directory = os.path.join(os.path.dirname(__file__), bronze_path)

print(directory)

file = get_latest_folder_with_file(directory)

df_bronze = spark.read.format("json") \
        .option("inferSchema", "true") \
        .option("multiline", "true") \
        .load(file)

if df_bronze is None:

    print("DataFrame bronze é None. Abortando ingestão silver.")

print("Schema do DataFrame bronze:")

df_bronze.printSchema()

print(f"Número de linhas no DataFrame bronze: {df_bronze.count()}")

df_bronze.show(truncate=False)

df_bronze = df_bronze.select(
    F.col("id").alias("brewery_id"),
    F.col("name").alias("brewery_name"),
    F.col("brewery_type"),
    F.concat(F.col("street"), F.lit(" "), F.col("address_1")).alias("address"),
    F.col("city"),
    F.col("state_province").alias("state"),
    F.col("postal_code"),
    F.col("country"),
    F.col("phone"),
    F.col("website_url"),
    F.col("longitude"),
    F.col("latitude")
).distinct()

if silver_path is None:
    print("Caminho silver não configurado. Abortando ingestão silver")

silver_path = os.path.join(os.path.dirname(os.path.abspath("../../")), silver_path)

print(silver_path)

target_path = os.path.join(silver_path, "brewery\\")

file_name = os.path.join(target_path)
try:
    df_bronze.write.mode("overwrite").parquet(file_name)
    print(f"DataFrame successfully saved as Parquet in: {file_name}")
except Exception as e:
    print(f"Error: {e}")