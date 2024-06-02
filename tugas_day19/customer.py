from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, window
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, TimestampType

spark = SparkSession.builder \
    .appName("PurchaseAggregator") \
    .getOrCreate()

schema = StructType([
    StructField("timestamp", StringType(), True),
    StructField("amount", DoubleType(), True)
])

kafka_stream = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "purchases") \
    .load()

parsed_stream = kafka_stream \
    .selectExpr("CAST(value AS STRING)") \
    .select(from_json(col("value"), schema).alias("data")) \
    .select("data.*")

parsed_stream = parsed_stream.withColumn("timestamp", (col("timestamp") / 1000).cast(TimestampType()))

agg_stream = parsed_stream \
    .groupBy(window(col("timestamp"), "1 minute", "1 minute")) \
    .sum("amount") \
    .withColumnRenamed("sum(amount)", "total_amount")

query = agg_stream \
    .select("window.start", "window.end", "total_amount") \
    .writeStream \
    .outputMode("complete") \
    .format("console") \
    .option("truncate", "false") \
    .start()

query.awaitTermination()
