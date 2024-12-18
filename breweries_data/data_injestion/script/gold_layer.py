from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.utils import AnalysisException
from spark_utils import iniciar_spark, fechar_spark

import utils
import os
from glob import glob

spark = iniciar_spark("visiaoAgregadaGold")

# Variaveis
layer_silver = "silver"
silver_path = utils.ler_configuracoes(layer_silver)
print(silver_path)
layer_gold = "gold"
gold_path = utils.ler_configuracoes(layer_gold)
print(gold_path)

directory = os.path.join(os.path.dirname(os.path.abspath("../../")), silver_path)
print(directory)

layer_silver = "silver"
silver_path = utils.ler_configuracoes(layer_silver)
layer_gold = "gold"
gold_path = utils.ler_configuracoes(layer_gold)

arquivo_silver = utils.obter_ultimo_parquet("silver")

if arquivo_silver:
    try:
        # Lê os dados da camada Silver
        df_silver = spark.read.parquet(arquivo_silver)

        # Processamento da camada Gold: Agregação
        df_gold = df_silver.groupBy("brewery_type", "city", "state", "country").agg(
            F.count("*").alias("num_breweries")  # Conta o número de cervejarias por grupo
        ).orderBy("country", "state", "city", "brewery_type") #Ordena os dados

        # Exibe os dados (opcional, para verificar o resultado)
        df_gold.show(truncate=False)

        # Obtém o caminho para salvar a camada Gold
        # arquivo_gold = utils.obter_ultimo_parquet("gold") # Obtém o caminho onde salvar o arquivo gold

        if gold_path is None:
            print("Caminho silver não configurado. Abortando ingestão silver")
        gold_path = os.path.join(os.path.dirname(os.path.abspath("../../")), gold_path)
        print(gold_path)

        target_path = os.path.join(gold_path, "brewery\\")

        file_name = os.path.join(target_path)

        df_gold.write.mode("overwrite").parquet(file_name)
        print(f"DataFrame successfully saved as Parquet in: {file_name}")

    except Exception as e:
        print(f"Erro no processamento da camada Gold: {e}")
else:
    print("Erro: Arquivo da camada Silver não encontrado.")