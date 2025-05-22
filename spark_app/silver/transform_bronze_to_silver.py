from pyspark.sql.functions import col, to_date
from utils.spark_session import create_spark_session

spark = create_spark_session("BronzeToSilver")

# Caminho bronze (staging)
bronze_path = "s3a://bigdata-finops-lake/staging/cotacoes_acoes"

# Caminho silver
silver_path = "s3a://bigdata-finops-lake/silver/cotacoes_acoes"

# Leitura do Parquet bruto
df = spark.read.parquet(bronze_path)

# Transformações básicas (ajuste conforme schema real)
df_clean = df.dropna(subset=["ticker", "preco", "data"]) \
    .withColumn("preco", col("preco").cast("double")) \
    .withColumn("data", to_date(col("data"), "yyyy-MM-dd"))

# Escrita particionada em Silver
df_clean.write.mode("overwrite") \
    .partitionBy("data") \
    .parquet(silver_path)

print("Transformação Bronze → Silver concluída.")

