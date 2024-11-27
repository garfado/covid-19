from pyspark.sql import SparkSession
from pyspark.sql.functions import col, year, sum

spark = SparkSession.builder.appName("Process to Trusted").getOrCreate()

# Carregar dados do Refined
df_vaccinations = spark.read.parquet("/home/daniel/covid/datalake/refined/vaccinations")
df_locations = spark.read.parquet("/home/daniel/covid/datalake/refined/locations")

# Selecione as colunas necessárias diretamente
df_vaccinations_with_year = df_vaccinations.select(
    col("country"),
    col("iso_code"),
    col("year"),
    col("total_vaccinations")
)

# Calcular total de vacinações por país e ano
df_total_vaccinations = df_vaccinations_with_year.groupBy("country", "iso_code", "year").agg(
    sum("total_vaccinations").alias("sum_total_vaccinations")
)

# Juntar com o DataFrame de Localizações para incluir número de vacinas
df_combined = df_total_vaccinations.join(
    df_locations.select(col("location").alias("country"), "vaccines"),
    on="country",
    how="inner"
)

# Salvar resultado no Trusted
output_path = "/home/daniel/covid/datalake/trusted/vaccinations"
df_combined.write.mode("overwrite").parquet(output_path)

print(f"Dados processados e salvos em: {output_path}")
