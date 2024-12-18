from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.utils import AnalysisException, IllegalArgumentException
from datetime import datetime
import json
import os
from glob import glob
import traceback

def ler_configuracoes(layer_name):
    """Lê o arquivo de configuração e retorna o caminho para o layer especificado."""
    nome_arquivo = "config.json"

    try:
        diretorio_atual = os.path.dirname(__file__)
        file_path = os.path.join(diretorio_atual, nome_arquivo)

        with open(file_path, 'r') as file:
            data = json.load(file)

            for item in data.get("layers", []):
                if item.get("layer") == layer_name:
                    return item.get("path")

        return None  # Retorna None se o layer não for encontrado

    except FileNotFoundError:
        print(f"Erro: Arquivo de configuração '{nome_arquivo}' não encontrado.")
        return None
    except json.JSONDecodeError:
        print(f"Erro: Arquivo de configuração '{nome_arquivo}' contém JSON inválido.")
        return None
    except Exception as e:
        print(f"Erro ao ler o arquivo de config: {e}")
        return None
   
# Save Parquet File
def save_to_parquet (dt, path):
    """
    Salva o dataframe para arquivos parquet organizados no diretorio por ano/mes/dia/hora/minuto 

    Args:
        dt : O dataframe para salvar
        path : Caminho base do diretorio para salvar o arquivo parquet
    """
    
    # Formata data atual para gerar o caminho do diretorio em modelo HIVE
    current_date = datetime.now()
    year = current_date.strftime("%Y")
    month = current_date.strftime("%m")
    day = current_date.strftime("%d")
    hour = current_date.strftime("%H")
    minute = current_date.strftime("%M")
    
    # Cria Estrutura de Diretorios
    target_path = os.path.join(path, year, month, day, hour, minute)
    os.makedirs(target_path, exist_ok=True)
    
    # Save to parquet
    parquet_file = os.path.join(target_path, "data.parquet")
    dt.to_parquet(parquet_file, index=False)
    print(f"DataFrame saved to {parquet_file}")

def save_to_json(json_file, path):
    """
    Salva o dataframe para arquivos parquet organizados no diretorio por ano/mes/dia/hora/minuto 

    Args:
        json_file : O dataframe para salvar
        path : Caminho base do diretorio para salvar o arquivo parquet
    """
    
    # Formata data atual para gerar o caminho do diretorio em modelo HIVE
    current_date = datetime.now()
    year = current_date.strftime("%Y")
    month = current_date.strftime("%m")
    day = current_date.strftime("%d")
    hour = current_date.strftime("%H")
    minute = current_date.strftime("%M")
    
    # Cria Estrutura de Diretorios
    target_path = os.path.join(path, year, month, day, hour, minute)
    os.makedirs(target_path, exist_ok=True)
    
    # Save to json
    file_destination = os.path.join(target_path, "data.json")
    with open(file_destination, 'w') as f:
        json.dump(json_file, f, indent=4)
    print(f"DataFrame saved to {file_destination}")
    
# Obtem caminho do ultimo arquivo importado se existir
def get_latest_folder_with_file(base_directory, target_file="data.json"):
    try:
        # Ensure the directory exists
        if not os.path.isdir(base_directory):
            raise FileNotFoundError(f"The directory {base_directory} does not exist.")

        # List all items in the directory
        items = [os.path.join(base_directory, d) for d in os.listdir(base_directory)]
        # Filter out only directories
        directories = [d for d in items if os.path.isdir(d)]

        if not directories:
            raise FileNotFoundError("No directories found in the specified base directory.")

        # Find the latest directory by modification time
        latest_directory = max(directories, key=os.path.getmtime)

        # Construct the full path to the target file
        target_file_path = os.path.join(latest_directory, target_file)

        # Check if the target file exists in the latest directory
        if os.path.isfile(target_file_path):
            return target_file_path
        else:
            #print(f"{target_file} not found in the latest directory: {latest_directory}")
            return get_latest_folder_with_file(latest_directory,target_file)
            #raise FileNotFoundError(f"{target_file} not found in the latest directory: {latest_directory}")

    except Exception as e:
        print(f"Error: {e}")
        return None

# Save
def save_delta_data(df, path, file_name, partition_col):
    try:
        target_path = os.path.join(path, file_name)
        os.makedirs(os.path.dirname(target_path), exist_ok=True)
        df.write.format("delta").mode("overwrite").partitionBy(partition_col).save(target_path)
        return True
    except AnalysisException as e:
        print(f"Erro de análise (schema/tipos) ao salvar em Delta: {e}")
        traceback.print_exc()
        return False
    except IllegalArgumentException as e:
        print(f"Erro de argumento (caminho/particionamento) ao salvar em Delta: {e}")
        traceback.print_exc()
        return False
    except Exception as e:
        print(f"Erro genérico ao salvar em Delta Lake: {e}")
        traceback.print_exc()
        return False
    
def obter_ultimo_parquet(layer_name):
    """Obtém o caminho para o arquivo Parquet mais recente de uma camada."""
    caminho_camada = ler_configuracoes(layer_name) # Chamada direta, sem import
    if not caminho_camada:
        print(f"Erro: Caminho para a camada '{layer_name}' não encontrado na configuração.")
        return None

    # Constrói o caminho absoluto usando o diretório de trabalho atual
    diretorio_camada = os.path.join(os.getcwd(), caminho_camada)

    if not os.path.exists(diretorio_camada):
        print(f"Erro: Diretório '{diretorio_camada}' não existe.")
        return None

    arquivos_parquet = glob(os.path.join(diretorio_camada, "*.parquet"))

    if arquivos_parquet:
        arquivos_parquet.sort(key=os.path.getmtime, reverse=True)
        ultimo_arquivo_parquet = arquivos_parquet[0]
        print(f"Arquivo Parquet encontrado para '{layer_name}': {ultimo_arquivo_parquet}")
        return ultimo_arquivo_parquet
    else:
        print(f"Erro: Nenhum arquivo Parquet encontrado em '{diretorio_camada}'.")
        return None