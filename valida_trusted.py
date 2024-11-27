from pyspark.sql import SparkSession

# Inicializa a sessão Spark
spark = SparkSession.builder.appName("Valida Trusted").getOrCreate()

# Caminho dos dados Trusted
trusted_path = "/home/daniel/covid/datalake/trusted/vaccinations"

# Lê os dados da camada Trusted
df_trusted = spark.read.parquet(trusted_path)

# Mostra as 10 primeiras linhas
df_trusted.show(10, truncate=False)

# Exibe o esquema dos dados
df_trusted.printSchema()

# Conta o número total de registros
total_registros = df_trusted.count()
print(f"Total de registros: {total_registros}")

