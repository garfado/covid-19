from pyspark.sql import SparkSession

# Inicializar Spark
spark = SparkSession.builder.appName("Process Refined to Trusted").getOrCreate()

# Caminhos
refined_locations_path = "/home/daniel/covid/datalake/refined/locations"
trusted_locations_path = "/home/daniel/covid/datalake/trusted/locations"

# Ler os dados da camada Refined
df_refined = spark.read.parquet(refined_locations_path)

# Exibir os dados para garantir que estão corretos
df_refined.show()

# Salvar na camada Trusted
df_refined.write.mode("overwrite").parquet(trusted_locations_path)

print(f"Dados processados e salvos em: {trusted_locations_path}")
