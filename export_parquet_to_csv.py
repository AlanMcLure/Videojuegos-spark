from pyspark.sql import SparkSession
from pyspark.sql.types import MapType, ArrayType, StructType
from pyspark.sql import functions as F
import os

INPUT_FOLDER = "data"
OUTPUT_FOLDER = "csv"

spark = SparkSession.builder.appName("ExportParquetToCSV").getOrCreate()
os.makedirs(OUTPUT_FOLDER, exist_ok=True)

for file in os.listdir(INPUT_FOLDER):
    if file.endswith(".parquet"):
        source_name = file.replace(".parquet", "")
        input_path = os.path.join(INPUT_FOLDER, file)
        output_path = os.path.join(OUTPUT_FOLDER, source_name)

        print(f"📦 Exportando {file} a CSV...")

        df = spark.read.parquet(input_path)

        # Convertir columnas complejas a string
        for field in df.schema.fields:
            if isinstance(field.dataType, (MapType, ArrayType, StructType)):
                df = df.withColumn(field.name, F.col(field.name).cast("string"))

        df.coalesce(1).write.option("header", "true").csv(output_path, mode="overwrite")

print("✅ Exportación completada.")
spark.stop()
