from datetime import datetime
from pyspark.sql import SparkSession, Row
from pyspark.sql.types import *
import logging
import os


def save_rdd_to_parquet(rdd, output_path: str, logger, source_name: str) -> dict:
    spark = SparkSession.builder.getOrCreate()
    started_at = datetime.utcnow()
    logger.info(f"[{source_name}] Iniciando guardado a Parquet: {output_path}")

    source_name = source_name.lower()

    if source_name == "showdown_pokedex":
        logger.info("[SHOWDOWN_POKEDEX] Aplicando esquema fijo para Pokédex Showdown")

        schema = StructType(
            [
                StructField("record_type", StringType(), True),
                StructField("pokemon_id", StringType(), True),
                StructField("name", StringType(), True),
                StructField("types", ArrayType(StringType()), True),
                StructField("baseStats", MapType(StringType(), IntegerType()), True),
                StructField("abilities", MapType(StringType(), StringType()), True),
                StructField("extraction_time", StringType(), True),
                StructField("source", StringType(), True),
                StructField("endpoint", StringType(), True),
            ]
        )

        required_keys = [field.name for field in schema]

        def normalize_pokedex(row: dict):
            return {k: row.get(k, None) for k in required_keys}

        rdd_filled = rdd.map(normalize_pokedex)
        df = spark.createDataFrame(
            rdd_filled.map(lambda row: Row(**row)), schema=schema
        )

    elif source_name in {"twitch_games", "twitch_streams", "hltb"}:
        logger.info(
            f"[{source_name.upper()}] Aplicando esquema inferido (datos limpios)"
        )
        df = spark.createDataFrame(rdd.map(lambda row: Row(**row)))

    else:
        logger.warning(
            f"[{source_name}] Fuente sin esquema específico, usando inferencia"
        )
        df = spark.createDataFrame(rdd.map(lambda row: Row(**row)))

    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    df.write.mode("overwrite").parquet(output_path)

    finished_at = datetime.utcnow()
    count = df.count()
    duration = (finished_at - started_at).total_seconds()

    logger.info(
        f"[{source_name}] Parquet guardado: registros={count}, tiempo={duration:.2f}s"
    )

    metadata = {
        "source": source_name,
        "output_file": output_path,
        "records": count,
        "time_sec": duration,
        "started_at": started_at,
        "finished_at": finished_at,
        "endpoint": f"parquet/{source_name}",
    }
    return metadata
