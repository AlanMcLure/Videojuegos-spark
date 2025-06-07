# main.py
from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession
from datetime import datetime
import logging
import os

from dotenv import load_dotenv

load_dotenv()

# Configuración de la base de datos
DB_URI = os.getenv("VG_CORE_DB_URI")
print(f"[DEBUG] DB_URI='{DB_URI}'")
if not DB_URI:
    raise RuntimeError("Falta definir VG_CORE_DB_URI en el archivo .env")

# Imports de nuestros módulos
from extract.twitch import extract_all_twitch
from extract.showdown import extract_all_showdown
from extract.howlongtobeat import extract_all_hltb
from carga.to_parquet import save_rdd_to_parquet
from carga.to_db import init_log_table, insert_log, load_to_core_schema
from transform.clean import clean_and_transform_data


def setup_spark(app_name="VGDataETL"):
    """Configurar Spark Context y Spark Session"""
    conf = SparkConf().setAppName(app_name)
    conf.set("spark.sql.adaptive.enabled", "true")
    conf.set("spark.sql.adaptive.coalescePartitions.enabled", "true")

    sc = SparkContext(conf=conf)
    spark = SparkSession(sc)
    return sc, spark


def setup_logger():
    """Configurar logging"""
    # Crear directorio de logs si no existe
    os.makedirs("logs", exist_ok=True)

    logger = logging.getLogger("vg_etl")
    logger.setLevel(logging.INFO)

    # Handler para archivo
    fh = logging.FileHandler("logs/vg_etl.log")
    fmt = logging.Formatter("%(asctime)s — %(levelname)s — %(message)s")
    fh.setFormatter(fmt)
    logger.addHandler(fh)

    # Handler para consola
    ch = logging.StreamHandler()
    ch.setFormatter(fmt)
    logger.addHandler(ch)

    return logger


def extract_phase(sc, logger):
    """Fase 1: Extracción de datos de todas las APIs"""
    logger.info("=== FASE 1: EXTRACCIÓN DE DATOS ===")

    extracted_data = {}

    try:
        # Twitch API
        logger.info("Extrayendo datos de Twitch API...")
        rdd_twitch = extract_all_twitch(sc, logger)
        games_list = (
            rdd_twitch.filter(lambda rec: rec["record_type"] == "game")
            .map(lambda rec: rec["game_name"])
            .distinct()
            .take(100)  # por ejemplo los top 100 títulos
        )
        extracted_data["twitch"] = rdd_twitch
        logger.info("Extracción de Twitch completada")

        # Pokémon Showdown API
        logger.info("Extrayendo datos de Pokémon Showdown API...")
        rdd_showdown = extract_all_showdown(sc, logger)
        extracted_data["showdown"] = rdd_showdown
        logger.info("Extracción de Showdown completada")

        # HowLongToBeat API
        logger.info("Extrayendo datos de HowLongToBeat API...")
        rdd_howlong = extract_all_hltb(sc, logger, games_list)
        extracted_data["howlongtobeat"] = rdd_howlong
        logger.info("Extracción de HowLongToBeat completada")

    except Exception as e:
        logger.error(f"Error en fase de extracción: {str(e)}")
        raise

    return extracted_data


def load_to_parquet_phase(extracted_data, logger, engine, logs_tbl):
    """Fase 1.5: Guardado a Parquet y logging"""
    logger.info("=== GUARDANDO A PARQUET ===")

    # Crear directorio de datos si no existe
    os.makedirs("data", exist_ok=True)

    parquet_metadata = {}

    for source_name, rdd_data in extracted_data.items():
        try:
            logger.info(f"Guardando {source_name} a Parquet...")
            parquet_path = f"data/{source_name}.parquet"

            # Guardar a Parquet y obtener metadatos
            meta = save_rdd_to_parquet(
                rdd_data, parquet_path, logger, source_name.upper()
            )
            parquet_metadata[source_name] = meta

            # Registrar en log de base de datos
            insert_log(engine, logs_tbl, meta)
            logger.info(f"Guardado de {source_name} completado")

        except Exception as e:
            logger.error(f"Error guardando {source_name} a Parquet: {str(e)}")
            raise

    return parquet_metadata


def transform_phase(spark, logger, engine, logs_tbl):
    """Fase 2: Limpieza y transformación de datos"""
    logger.info("=== FASE 2: LIMPIEZA Y TRANSFORMACIÓN ===")

    transformed_data = {}

    try:
        # Leer datos de Parquet y limpiar
        for source in ["twitch", "showdown", "howlongtobeat"]:
            logger.info(f"Limpiando datos de {source}...")

            parquet_path = f"data/{source}.parquet"
            df_raw = spark.read.parquet(parquet_path)

            # Aplicar limpieza y transformación
            df_clean = clean_and_transform_data(df_raw, source, logger)
            transformed_data[source] = df_clean

            logger.info(f"Limpieza de {source} completada")

            # Log de transformación
            transform_meta = {
                "load_start_time": datetime.utcnow(),
                "load_end_time": datetime.utcnow(),
                "records_count": df_clean.count(),
                "source": source.upper(),
                "endpoint": f"transform/{source}",
                "output_file": f"{source}_cleaned",
            }
            insert_log(engine, logs_tbl, transform_meta)

    except Exception as e:
        logger.error(f"Error en fase de transformación: {str(e)}")
        raise

    return transformed_data


def load_to_db_phase(transformed_data, logger, engine, logs_tbl):
    """Fase 3: Carga a base de datos (schema core)"""
    logger.info("=== FASE 3: CARGA A BASE DE DATOS ===")

    try:
        for source, df_clean in transformed_data.items():
            logger.info(f"Cargando {source} a base de datos...")

            # Cargar al schema core
            load_meta = load_to_core_schema(df_clean, source, engine, logger)

            # Registrar en log
            insert_log(engine, logs_tbl, load_meta)

            logger.info(f"Carga de {source} a BD completada")

    except Exception as e:
        logger.error(f"Error en fase de carga: {str(e)}")
        raise


def main():
    """Función principal del pipeline ETL"""
    # Configuración inicial
    sc, spark = setup_spark()
    logger = setup_logger()

    start_time = datetime.utcnow()
    logger.info("=== INICIO DEL PIPELINE ETL VIDEOJUEGOS ===")
    logger.info(f"Tiempo de inicio: {start_time}")

    try:
        # Inicializar base de datos y tabla de logs
        logger.info("Inicializando base de datos y tabla de logs...")
        engine, logs_tbl = init_log_table(DB_URI)
        logger.info("Base de datos inicializada correctamente")

        # FASE 1: Extracción
        extracted_data = extract_phase(sc, logger)

        # FASE 1.5: Guardado a Parquet
        parquet_metadata = load_to_parquet_phase(
            extracted_data, logger, engine, logs_tbl
        )

        # FASE 2: Transformación y limpieza
        transformed_data = transform_phase(spark, logger, engine, logs_tbl)

        # FASE 3: Carga a base de datos
        load_to_db_phase(transformed_data, logger, engine, logs_tbl)

        # Finalización
        end_time = datetime.utcnow()
        total_seconds = (end_time - start_time).total_seconds()

        logger.info("=== PIPELINE COMPLETADO EXITOSAMENTE ===")
        logger.info(f"Tiempo total de ejecución: {total_seconds:.2f} segundos")
        logger.info(f"Tiempo de finalización: {end_time}")

        # Log final del pipeline completo
        pipeline_meta = {
            "load_start_time": start_time,
            "load_end_time": end_time,
            "records_count": sum(
                [meta.get("records_count", 0) for meta in parquet_metadata.values()]
            ),
            "source": "PIPELINE_COMPLETE",
            "endpoint": "main.py",
            "output_file": "complete_pipeline",
        }
        insert_log(engine, logs_tbl, pipeline_meta)

    except Exception as e:
        logger.error(f"Error crítico en el pipeline: {str(e)}")
        logger.error("Pipeline terminado con errores")
        raise

    finally:
        # Limpiar recursos
        logger.info("Cerrando conexiones y limpiando recursos...")
        sc.stop()
        logger.info("Pipeline finalizado")


if __name__ == "__main__":
    main()
