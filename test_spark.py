# test_spark.py
from pyspark.sql import SparkSession
import os

# Crear directorio data si no existe
os.makedirs("data", exist_ok=True)

# Crear sesión de Spark
spark = SparkSession.builder.appName("TestParquet").getOrCreate()

# Crear un DataFrame de prueba
df = spark.createDataFrame([(1, "hola"), (2, "mundo")], ["id", "mensaje"])

# Guardar como Parquet
df.write.mode("overwrite").parquet("data/test_output.parquet")

print("✅ Guardado correctamente en Parquet.")
