from pyspark.sql import SparkSession
from pyspark.sql.functions import col, explode, split, size, year

# SparkSession
spark = SparkSession.builder.appName("Process to Refined").getOrCreate()

# Raw e Refined
raw_path = "/home/daniel/covid/datalake/raw"
refined_path = "/home/daniel/covid/datalake/refined"

# Refined existe
import os
os.makedirs(refined_path, exist_ok=True)

# processar locations.csv
def process_locations():
    locations_file = f"{raw_path}/locations.csv"
    df_locations = spark.read.option("header", True).csv(locations_file)

    # coluna com o número de vacinas disponíveis por país
    df_locations = df_locations.withColumn("num_vaccines", 
                                           size(split(col("vaccines"), ", ")))

    # DataFrame transformado na camada Refined
    output_path = f"{refined_path}/locations"
    df_locations.write.mode("overwrite").parquet(output_path)
    print(f"Arquivo locations processado e salvo em {output_path}")

# processar vaccinations.json
def process_vaccinations():
    vaccinations_file = f"{raw_path}/vaccinations.json"
    df_vaccinations = spark.read.option("multiline", True).json(vaccinations_file)

    # registros por país, ano e total de vacinaçoes
    df_vaccinations_exploded = df_vaccinations.withColumn("data_exploded", explode(col("data")))

    df_vaccinations_with_year = df_vaccinations_exploded.select(
        col("country"),
        col("iso_code"),
        year(col("data_exploded.date")).alias("year"),
        col("data_exploded.total_vaccinations").alias("total_vaccinations")
    )

    # DataFrame transformado Refined
    output_path = f"{refined_path}/vaccinations"
    df_vaccinations_with_year.write.mode("overwrite").parquet(output_path)
    print(f"Arquivo vaccinations processado e salvo em {output_path}")

# arquivos
process_locations()
process_vaccinations()
