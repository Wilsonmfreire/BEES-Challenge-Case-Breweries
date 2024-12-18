from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from spark_utils import iniciar_spark, fechar_spark
import pandas as pd
import requests

import utils
spark = iniciar_spark("ingestaoBronze")

# Variaveis
layer = "bronze"
layer_path = "" 

def realizar_requisicao():
    """Realiza a requisição à API e retorna os dados em JSON."""
    url = "https://api.openbrewerydb.org/v1/breweries"
    try:
        response = requests.get(url)
        response.raise_for_status()  # Lança uma exceção para status code diferente de 2xx
        return response.json()
    except requests.exceptions.RequestException as e:
        print(f"Erro na requisição: {e}")
        return None
    
def ingestao_bronze():

    layer_path = utils.ler_configuracoes(layer)

    try:
        raw_data = realizar_requisicao()

        if raw_data:
            print("Dados recebidos da API com sucesso!")  
            try:
                utils.save_to_json(raw_data, layer_path)
                
            except Exception as e:
                print(f"Erro ao processar dados: {e}")
        else:
            print("Não foi possível obter dados da API.")
    except Exception as e:
        print(f"Erro geral na ingestão: {e}")  

if __name__ == "__main__":
    ingestao_bronze()