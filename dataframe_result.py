import pandas as pd

# Carregar o CSV consolidado
df = pd.read_csv("/home/daniel/covid/datalake/combined_results_final.csv")

# Visualizar as primeiras linhas
print(df.head(10))

