from pyspark.sql import SparkSession, functions as F

spark = SparkSession.builder.appName("Tarea3_Batch_Analisis").getOrCreate()
spark.sparkContext.setLogLevel("WARN")

ruta = "hdfs://localhost:9000/Tarea3/rows.csv"
df = (spark.read.format("csv")
      .option("header", "true")
      .option("inferSchema", "true")
      .load(ruta))

print("\n=== ESTRUCTURA DEL CONJUNTO DE DATOS ===")
df.printSchema()
print("\n=== PRIMEROS REGISTROS ===")
df.show(20, truncate=False)
print("\n=== ESTADISTICAS DESCRIPTIVAS ===")
df.summary().show(truncate=False)
print("\n=== REGISTROS CON VALOR SUPERIOR A 5000 ===")
dias = df.filter(F.col("VALOR") > 5000).select("VALOR", "VIGENCIADESDE", "VIGENCIAHASTA")
dias.show(truncate=False)
spark.stop()
