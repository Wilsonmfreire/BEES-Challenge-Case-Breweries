# spark_utils.py
import os
import findspark
from pyspark.sql import SparkSession

def iniciar_spark(app_name="MyApp"):
    """Inicializa uma sessão Spark com configuração de ambiente.

    Args:
        app_name (str, optional): Nome da aplicação Spark. Padrão: "MyApp".

    Returns:
        SparkSession: A sessão Spark inicializada ou None em caso de erro.
    """
    try:
        # Obtém as variáveis de ambiente HADOOP_HOME e SPARK_HOME
        hadoop_home = os.environ.get("HADOOP_HOME")
        spark_home = os.environ.get("SPARK_HOME")

        # Verifica se as variáveis de ambiente estão definidas
        if not hadoop_home:
            raise ValueError("Variável de ambiente HADOOP_HOME não definida. Configure o ambiente conforme o README.")
        if not spark_home:
            raise ValueError("Variável de ambiente SPARK_HOME não definida. Configure o ambiente conforme o README.")

        # Configura o PATH para incluir o diretório bin do Hadoop (importante para Windows)
        os.environ["PATH"] = os.environ["PATH"] + ";" + os.path.join(hadoop_home, "bin")

        # Inicializa o findspark com o caminho do Spark
        findspark.init(spark_home)

        # Cria e retorna a sessão Spark
        spark = SparkSession.builder.appName(app_name).getOrCreate()
        print(f"Sessão Spark iniciada com o nome: {app_name}")
        return spark

    except ValueError as ve:
        print(f"Erro de configuração: {ve}")
        return None
    except Exception as e:
        print(f"Erro ao iniciar o Spark: {e}")
        return None

def fechar_spark(spark):
    """Finaliza uma sessão Spark.

    Args:
        spark (SparkSession): A sessão Spark a ser finalizada.
    """
    if spark:
        spark.stop()
        print("Sessão Spark finalizada.")