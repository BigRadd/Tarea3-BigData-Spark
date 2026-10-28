from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, window, timestamp_seconds
from pyspark.sql.types import StructType, StructField, IntegerType, FloatType

spark = (SparkSession.builder.appName("KafkaSparkStreaming").getOrCreate())
spark.sparkContext.setLogLevel("WARN")

schema = StructType([
    StructField("sensor_id", IntegerType(), True),
    StructField("temperature", FloatType(), True),
    StructField("humidity", FloatType(), True),
    StructField("timestamp", IntegerType(), True)
])

stream = (spark.readStream.format("kafka")
          .option("kafka.bootstrap.servers", "localhost:9092")
          .option("subscribe", "sensor_data")
          .option("startingOffsets", "latest")
          .load())

parsed = stream.select(from_json(col("value").cast("string"), schema).alias("data")).select("data.*")
parsed = parsed.withColumn("timestamp", timestamp_seconds(col("timestamp")))

stats = (parsed.groupBy(window(col("timestamp"), "1 minute"), col("sensor_id"))
         .agg({"temperature": "avg", "humidity": "avg"}))

query = (stats.writeStream.outputMode("complete").format("console")
         .option("truncate", "false").start())
query.awaitTermination()
