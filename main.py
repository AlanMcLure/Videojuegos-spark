import sys
import io
from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession
from datetime import datetime
import logging
import os

from dotenv import load_dotenv

load_dotenv()

DB_URI = os.getenv("VG_CORE_DB_URI")
if not DB_URI:
    raise RuntimeError("Falta definir VG_CORE_DB_URI en el archivo .env")

DB_JDBC_URI = os.getenv("VG_CORE_DB_JDBC_URI")
if not DB_JDBC_URI:
    raise RuntimeError("Falta definir VG_CORE_DB_JDBC_URI en el archivo .env")

from extract.twitch import extract_all_twitch
from extract.showdown import extract_all_showdown
from extract.howlongtobeat import extract_all_hltb
from carga.to_parquet import save_rdd_to_parquet
from carga.to_db import init_log_table, insert_log, load_to_core_schema
from transform.clean import clean_and_transform_data

extracted_data = {}
transformed_data = {}
parquet_metadata = {}
load_id = int(datetime.utcnow().timestamp())


def setup_spark(app_name="VGDataETL"):
    conf = SparkConf().setAppName(app_name)
    conf.set("spark.sql.adaptive.enabled", "true")
    conf.set("spark.sql.adaptive.coalescePartitions.enabled", "true")
    sc = SparkContext(conf=conf)
    spark = SparkSession(sc)
    return sc, spark


def setup_logger():
    os.makedirs("logs", exist_ok=True)
    logger = logging.getLogger("vg_etl")
    logger.setLevel(logging.INFO)

    if not logger.handlers:
        fh = logging.FileHandler("logs/vg_etl.log")
        fmt = logging.Formatter("%(asctime)s — %(levelname)s — %(message)s")
        fh.setFormatter(fmt)
        logger.addHandler(fh)

        sys.stdout = io.TextIOWrapper(
            sys.stdout.detach(), encoding="utf-8", line_buffering=True
        )
        sys.stderr = io.TextIOWrapper(
            sys.stderr.detach(), encoding="utf-8", line_buffering=True
        )

        ch = logging.StreamHandler()
        ch.setFormatter(fmt)
        logger.addHandler(ch)

    return logger


def extract_phase(sc, logger):
    logger.info("=== FASE 1: EXTRACCIÓN DE DATOS ===")
    global extracted_data
    extracted_data = {}

    try:
        logger.info("Extrayendo datos de Twitch...")
        rdd_twitch_games, rdd_twitch_streams = extract_all_twitch(sc, logger)
        games_list = (
            rdd_twitch_games.map(lambda rec: rec["game_name"]).distinct().take(100)
        )
        extracted_data["twitch_games"] = rdd_twitch_games
        extracted_data["twitch_streams"] = rdd_twitch_streams

        logger.info("Extrayendo datos de Showdown...")
        rdd_showdown_pokedex = extract_all_showdown(sc, logger)
        extracted_data["showdown_pokedex"] = rdd_showdown_pokedex

        logger.info("Extrayendo datos de HowLongToBeat...")
        rdd_howlong = extract_all_hltb(sc, logger, games_list)
        extracted_data["howlongtobeat"] = rdd_howlong

        for source, rdd in extracted_data.items():
            logger.info(f"Muestra de datos extraídos para {source.upper()}:")
            sample = rdd.take(5)
            for i, record in enumerate(sample):
                logger.info(f"[{source}] Ejemplo {i + 1}: {record}")

        logger.info("Fase de extracción completada con éxito.")
    except Exception as e:
        logger.error(f"Error en fase de extracción: {str(e)}")
        raise


def load_to_parquet_phase(logger, engine):
    logger.info("=== GUARDANDO A PARQUET ===")
    global extracted_data, parquet_metadata

    os.makedirs("data", exist_ok=True)
    parquet_metadata = {}

    for source_name, rdd_data in extracted_data.items():
        try:
            logger.info(f"Guardando {source_name} a Parquet...")
            parquet_path = f"data/{source_name}.parquet"

            meta = save_rdd_to_parquet(
                rdd_data, parquet_path, logger, source_name.upper()
            )
            parquet_metadata[source_name] = meta

            insert_log(engine, meta)
            logger.info(f"Guardado de {source_name} completado")
        except Exception as e:
            logger.error(f"Error guardando {source_name} a Parquet: {str(e)}")
            raise


def transform_phase(spark, logger, engine, load_id):
    logger.info("=== FASE 2: TRANSFORMACIÓN ===")
    global transformed_data
    transformed_data = {}

    try:
        for source in extracted_data.keys():
            logger.info(f"Limpieza de {source}...")
            parquet_path = f"data/{source}.parquet"
            df_raw = spark.read.parquet(parquet_path)
            df_clean = clean_and_transform_data(df_raw, source, logger)
            transformed_data[source] = df_clean

            transform_meta = {
                "load_start_time": datetime.utcnow(),
                "load_end_time": datetime.utcnow(),
                "records_count": df_clean.count(),
                "source": source.upper(),
                "endpoint": f"transform/{source}",
                "output_file": f"{source}_cleaned",
            }
            insert_log(engine, transform_meta)

        logger.info("Fase de transformación completada con éxito.")
    except Exception as e:
        logger.error(f"Error en fase de transformación: {str(e)}")
        raise


def load_to_db_phase(logger, engine):
    logger.info("=== FASE 3: CARGA A BD ===")
    global transformed_data

    # Mapeo de nombres internos de fuentes a nombres base usados en las funciones de carga
    source_map = {
        "twitch_games": "twitch",
        "twitch_streams": "twitch",
        "showdown_pokedex": "showdown_pokedex",
        "howlongtobeat": "howlongtobeat",
    }

    try:
        for source_name, df_clean in transformed_data.items():
            logger.info(f"Cargando {source_name} a base de datos...")

            # Obtener nombre base que entiende `load_to_core_schema`
            mapped_source = source_map.get(source_name)
            if not mapped_source:
                logger.warning(f"Fuente no mapeada: {source_name}. Saltando carga...")
                continue

            # Ejecutar carga
            load_meta = load_to_core_schema(
                df_clean, mapped_source, DB_JDBC_URI, logger
            )

            # Guardar log
            log_id = insert_log(engine, load_meta)
            logger.info(f"Carga completada para {source_name}. ID de log: {log_id}")

        logger.info("Fase de carga completada con éxito.")
    except Exception as e:
        logger.error(f"Error en fase de carga: {str(e)}")
        raise


def main():
    sc, spark = setup_spark()
    logger = setup_logger()

    logger.info("=== INICIO DEL PIPELINE ===")
    logger.info("Configurando conexión a la base de datos...")

    engine = init_log_table(DB_URI)

    try:
        extract_phase(sc, logger)
        load_to_parquet_phase(logger, engine)
        transform_phase(spark, logger, engine, load_id)
        load_to_db_phase(logger, engine)
    finally:
        sc.stop()
        logger.info("Pipeline finalizado")


if __name__ == "__main__":
    main()
