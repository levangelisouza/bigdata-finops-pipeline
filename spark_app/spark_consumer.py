from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, to_date
from pyspark.sql.types import StructType, StringType, DoubleType, TimestampType
import os

# Leitura das variáveis de ambiente
aws_access_key = os.getenv("AWS_ACCESS_KEY_ID")
aws_secret_key = os.getenv("AWS_SECRET_ACCESS_KEY")

# Inicializa SparkSession
spark = SparkSession.builder \
    .appName("KafkaSparkS3Consumer") \
    .config("spark.hadoop.fs.s3a.access.key", aws_access_key) \
    .config("spark.hadoop.fs.s3a.secret.key", aws_secret_key) \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .getOrCreate()

spark.sparkContext.setLogLevel("INFO")

# Schema do JSON recebido do Kafka
schema = StructType() \
    .add("ticker", StringType()) \
    .add("preco", DoubleType()) \
    .add("data", TimestampType())  # Corrigido para interpretar ISO 8601

# Leitura do Kafka
df_raw = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:9092") \
    .option("subscribe", "cotacoes-acoes") \
    .option("startingOffsets", "earliest") \
    .load()

# Conversão do campo value para colunas estruturadas
df = df_raw.selectExpr("CAST(value AS STRING) as json") \
    .select(from_json(col("json"), schema).alias("dados")) \
    .select("dados.*")

# Criação da coluna de partição
df = df.withColumn("data_particao", to_date(col("data")))

# Escrita no S3 particionada por data
query = df.writeStream \
    .format("parquet") \
    .option("checkpointLocation", "s3a://bigdata-finops-lake/checkpoints/cotacoes_acoes") \
    .option("path", "s3a://bigdata-finops-lake/staging/cotacoes_acoes") \
    .partitionBy("data_particao") \
    .outputMode("append") \
    .start()

query.awaitTermination()

