from pyspark.sql import SparkSession
from pyspark.sql.functions import col, year, sum

spark = SparkSession.builder.appName("Process to Trusted").getOrCreate()

# Carregar Refined
df_vaccinations = spark.read.parquet("/home/daniel/covid/datalake/refined/vaccinations")
df_locations = spark.read.parquet("/home/daniel/covid/datalake/refined/locations")

# Selecione as colunas
df_vaccinations_with_year = df_vaccinations.select(
    col("country"),
    col("iso_code"),
    col("year"),
    col("total_vaccinations")
)

# vacina por pais e ano
df_total_vaccinations = df_vaccinations_with_year.groupBy("country", "iso_code", "year").agg(
    sum("total_vaccinations").alias("sum_total_vaccinations")
)

# DataFrame de Localizacao para incluir numero de vacinas
df_combined = df_total_vaccinations.join(
    df_locations.select(col("location").alias("country"), "vaccines"),
    on="country",
    how="inner"
)

# Trusted
output_path = "/home/daniel/covid/datalake/trusted/vaccinations"
df_combined.write.mode("overwrite").parquet(output_path)

print(f"Dados processados e salvos em: {output_path}")
