from pyspark.sql import SparkSession
from pyspark.sql.functions import col, explode, size, split, desc, year, sum

# Inicializar Spark
spark = SparkSession.builder.appName("Combine Datasets").getOrCreate()

# Caminhos dos arquivos
locations_path = "/home/daniel/covid/datalake/locations.csv"
vaccinations_path = "/home/daniel/covid/datalake/vaccinations.json"

# Leitura dos arquivos
df_locations = spark.read.option("header", True).csv(locations_path)
df_vaccinations = spark.read.option("multiline", "true").json(vaccinations_path)

# Explodir a coluna 'data'
df_vaccinations_exploded = df_vaccinations.withColumn("data_exploded", explode(col("data")))

# Selecionar colunas necessárias do vaccinations.json
df_vaccinations_clean = df_vaccinations_exploded.select(
    col("country"),
    col("iso_code"),
    col("data_exploded.total_vaccinations").alias("total_vaccinations"),
    year(col("data_exploded.date")).alias("year")
)

# Agrupar por país e ano
df_vaccinations_grouped = (
    df_vaccinations_clean
    .groupBy("country", "iso_code", "year")
    .agg(sum("total_vaccinations").alias("sum_total_vaccinations"))
)

# Unir os DataFrames pelo iso_code
df_combined = df_vaccinations_grouped.join(df_locations, "iso_code", "inner")

# Adicionar coluna com número de vacinas usadas
df_combined = df_combined.withColumn("num_vaccines", size(split(col("vaccines"), ", ")))

# Ordenar por mais vacinas usadas e mais vacinações
df_combined_ordered = df_combined.orderBy(desc("num_vaccines"), desc("sum_total_vaccinations"))

# Mostrar os 10 primeiros países
df_combined_ordered.show(10, truncate=False)
