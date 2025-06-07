# load/to_parquet.py
from datetime import datetime
from pyspark.sql import SparkSession
import logging


def save_rdd_to_parquet(rdd, output_path: str, logger, source_name: str) -> dict:
    """
    Guarda un RDD de diccionarios en un fichero Parquet y devuelve metadata.

    Parámetros:
    - rdd: RDD de diccionarios a guardar.
    - output_path: ruta de salida (por ejemplo, 'data/twitch.parquet').
    - logger: logger de Python para registrar info.
    - source_name: nombre de la fuente (por ejemplo, 'TWITCH').

    Retorna un dict con claves:
    - 'source', 'output_file', 'records', 'time_sec', 'started_at', 'finished_at'.
    """
    spark = SparkSession.builder.getOrCreate()
    # Convertir a DataFrame
    df = spark.createDataFrame(rdd)

    # Iniciar temporización
    started_at = datetime.utcnow()
    logger.info(f"[{source_name}] Iniciando guardado a Parquet: {output_path}")

    # Guardar usando overwrite para evitar conflictos
    df.write.mode("overwrite").parquet(output_path)

    # Finalizar temporización
    finished_at = datetime.utcnow()
    count = df.count()
    duration = (finished_at - started_at).total_seconds()

    logger.info(
        f"[{source_name}] Parquet guardado: registros={count}, tiempo={duration:.2f}s"
    )

    # Construir metadata
    metadata = {
        "source": source_name,
        "output_file": output_path,
        "records": count,
        "time_sec": duration,
        "started_at": started_at,
        "finished_at": finished_at,
        "endpoint": f"parquet/{source_name.lower()}",
    }
    return metadata
