from pyspark.sql import SparkSession

spark = (
    SparkSession.builder.appName("TestParquet")
    .config("spark.hadoop.io.native.lib.available", "false")
    .getOrCreate()
)

df = spark.createDataFrame([(1, "test"), (2, "ok")], ["id", "value"])

# Ruta absoluta con esquema file:///
df.write.mode("overwrite").parquet(
    "file:///C:/Users/alanm/Desktop/IABD/Sistemas-Big-Data/Proyecto-final/test_output.parquet"
)

print("✅ Guardado correcto.")
