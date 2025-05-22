import sys
sys.path.append("/opt/spark_app")

from pyspark.sql.functions import col, avg
from utils.spark_session import create_spark_session

spark = create_spark_session("SilverToGold")

silver_path = "s3a://bigdata-finops-lake/silver/cotacoes_acoes"
gold_path = "s3a://bigdata-finops-lake/gold/indicadores_mensais"

df = spark.read.parquet(silver_path)

df_gold = df.groupBy("ticker", "data") \
    .agg(
        avg(col("preco")).alias("preco_medio")
    )

df_gold.write.mode("overwrite") \
    .partitionBy("data") \
    .parquet(gold_path)

print("Agregação Silver → Gold concluída.")

