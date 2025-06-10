from datetime import datetime
from pyspark.sql import SparkSession, Row
from pyspark.sql.types import *
import logging
import os


def save_rdd_to_parquet(rdd, output_path: str, logger, source_name: str) -> dict:
    spark = SparkSession.builder.getOrCreate()
    started_at = datetime.utcnow()
    logger.info(f"[{source_name}] Iniciando guardado a Parquet: {output_path}")

    if source_name.lower() == "showdown_ladder":
        logger.info(
            "[SHOWDOWN_LADDER] Aplicando esquema fijo para evitar errores de tipo"
        )

        schema = StructType(
            [
                StructField("record_type", StringType(), True),
                StructField("format", StringType(), True),
                StructField("username", StringType(), True),
                StructField("rating", DoubleType(), True),
                StructField("gxe", DoubleType(), True),
                StructField("pr", DoubleType(), True),
                StructField("levels", MapType(StringType(), IntegerType()), True),
                StructField("extraction_time", StringType(), True),
                StructField("source", StringType(), True),
                StructField("endpoint", StringType(), True),
            ]
        )

        required_keys = [field.name for field in schema]

        def normalize_and_cast(row: dict):
            safe_row = {k: row.get(k, None) for k in required_keys}
            for key in ["rating", "gxe", "pr"]:
                try:
                    safe_row[key] = (
                        float(safe_row[key]) if safe_row[key] is not None else None
                    )
                except (ValueError, TypeError):
                    safe_row[key] = None
            return safe_row

        rdd_filled = rdd.map(normalize_and_cast)
        df = spark.createDataFrame(
            rdd_filled.map(lambda row: Row(**row)), schema=schema
        )

    elif source_name.lower() == "showdown_profiles":
        logger.info(
            "[SHOWDOWN_PROFILES] Aplicando esquema fijo para evitar errores de inferencia"
        )

        schema = StructType(
            [
                StructField("record_type", StringType(), True),
                StructField("username", StringType(), True),
                StructField("format", StringType(), True),
                StructField("gxe", DoubleType(), True),
                StructField("rating", DoubleType(), True),
                StructField("levels", MapType(StringType(), IntegerType()), True),
                StructField("extraction_time", StringType(), True),
                StructField("source", StringType(), True),
                StructField("endpoint", StringType(), True),
            ]
        )

        required_keys = [
            "record_type",
            "username",
            "format",
            "gxe",
            "rating",
            "levels",
            "extraction_time",
            "source",
            "endpoint",
        ]

        def fill_missing_keys(row: dict):
            return {k: row.get(k, None) for k in required_keys}

        rdd_filled = rdd.map(fill_missing_keys)

        df = spark.createDataFrame(
            rdd_filled.map(lambda row: Row(**row)), schema=schema
        )

    elif source_name.lower() == "twitch_games":
        logger.info("[TWITCH_GAMES] Aplicando esquema inferido (datos limpios)")
        df = spark.createDataFrame(rdd.map(lambda row: Row(**row)))

    elif source_name.lower() == "twitch_streams":
        logger.info("[TWITCH_STREAMS] Aplicando esquema inferido (datos limpios)")
        df = spark.createDataFrame(rdd.map(lambda row: Row(**row)))

    elif source_name.lower() == "hltb":
        logger.info("[HLTB] Aplicando esquema inferido (datos limpios)")
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
        "endpoint": f"parquet/{source_name.lower()}",
    }
    return metadata
