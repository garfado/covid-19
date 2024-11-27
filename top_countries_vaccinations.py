from pyspark.sql import SparkSession
from pyspark.sql.functions import col, explode, sum, year, month, desc

# Inicializar Spark
spark = SparkSession.builder.appName("Top Countries Vaccinations").getOrCreate()

# Caminho do arquivo JSON
vaccinations_path = "/home/daniel/covid/datalake/vaccinations.json"

# Leitura do JSON
df_vaccinations = spark.read.option("multiline", "true").json(vaccinations_path)

# Explodir a coluna 'data'
df_vaccinations_exploded = df_vaccinations.withColumn("data_exploded", explode(col("data")))

# Selecionar colunas necessárias
df_vaccinations_clean = df_vaccinations_exploded.select(
    col("country"),
    col("iso_code"),
    col("data_exploded.total_vaccinations").alias("total_vaccinations"),
    year(col("data_exploded.date")).alias("year"),
    month(col("data_exploded.date")).alias("month")
)

# Filtrar para excluir 'World'
df_vaccinations_filtered = df_vaccinations_clean.filter(col("country") != "World")

# Agrupar por país, ano e mês, somando as vacinações
df_top_countries = (
    df_vaccinations_filtered
    .groupBy("country", "year", "month")
    .agg(sum("total_vaccinations").alias("sum_total_vaccinations"))
    .orderBy(desc("sum_total_vaccinations"))
)

# Selecionar os 10 principais países por mês e ano
df_top_10 = df_top_countries.limit(10)

# Mostrar o resultado
df_top_10.show(truncate=False)

