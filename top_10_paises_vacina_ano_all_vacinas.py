from pyspark.sql import SparkSession
from pyspark.sql.functions import col, split, size

# Inicializar Spark
spark = SparkSession.builder.appName("Top 10 Countries Vaccinations Analysis").getOrCreate()

# Caminhos dos arquivos Trusted
trusted_vaccinations_path = "/home/daniel/covid/datalake/trusted/vaccinations"
trusted_locations_path = "/home/daniel/covid/datalake/trusted/locations"

# Ler os arquivos Trusted
df_vaccinations = spark.read.parquet(trusted_vaccinations_path)
df_locations = spark.read.parquet(trusted_locations_path)

# Renomear coluna "vaccines" no DataFrame de localizações
df_locations = df_locations.withColumnRenamed("vaccines", "location_vaccines")

# Junta com o DataFrame de localizações para incluir o número de vacinas
df_combined = df_vaccinations.join(
    df_locations.select(col("location").alias("country"), "location_vaccines"),
    on="country",
    how="inner"
)

# Conta o número de vacinas usadas por cada país
df_combined = df_combined.withColumn("num_vaccines", size(split(col("location_vaccines"), ", ")))

# Ordena para obter os top 10 por ano
df_combined_ordered = df_combined.orderBy(col("sum_total_vaccinations").desc(), col("num_vaccines").desc())

# Mostra os resultados
df_combined_ordered.show(10, truncate=False)

# Salvar o DataFrame combinado como CSV
output_path = "/home/daniel/covid/datalake/trusted/combined_results.csv"
df_combined_ordered.write.csv(output_path, header=True)

print(f"Arquivo salvo em: {output_path}")

