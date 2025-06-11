from pyspark.sql.functions import lit
from sqlalchemy import create_engine, text
from datetime import datetime
import logging
from pyspark.sql.types import NullType


def init_log_table(db_uri: str):
    try:
        engine = create_engine(db_uri)
        with engine.connect() as conn:
            conn.execute(text("SELECT 1"))
        return engine
    except Exception as e:
        logging.error(f"Error inicializando base de datos: {str(e)}")
        raise


def insert_log(engine, log_data: dict):
    try:
        log_record = {
            "load_start_time": log_data.get("load_start_time", datetime.utcnow()),
            "load_end_time": log_data.get("load_end_time", datetime.utcnow()),
            "records_count": log_data.get("records_count", 0),
            "source": log_data.get("source", "UNKNOWN"),
            "endpoint": log_data.get("endpoint"),
            "output_file": log_data.get("output_file"),
            "status": log_data.get("status", "SUCCESS"),
            "error_message": log_data.get("error_message"),
        }

        with engine.connect() as conn:
            result = conn.execute(
                text("""
                    INSERT INTO logs.etl_logs (
                        load_start_time, load_end_time, records_count, source, endpoint,
                        output_file, status, error_message
                    ) VALUES (
                        :load_start_time, :load_end_time, :records_count, :source, :endpoint,
                        :output_file, :status, :error_message
                    ) RETURNING id
                """),
                log_record,
            )
            conn.commit()
            return result.scalar()

    except Exception as e:
        logging.error(f"Error insertando log: {str(e)}")
        raise


def drop_invalid_columns(df, logger):
    """
    Elimina columnas con tipo NullType o no compatibles con JDBC.
    """
    invalid_cols = [
        f.name for f in df.schema.fields if isinstance(f.dataType, NullType)
    ]
    if invalid_cols:
        logger.warning(f"Eliminando columnas inválidas para JDBC: {invalid_cols}")
        df = df.drop(*invalid_cols)
    return df


def load_to_core_schema(df, source: str, jdbc_uri: str, logger):
    start_time = datetime.utcnow()
    total_records = 0
    source_upper = source.upper()
    endpoint = f"load/core_{source.lower()}"
    output_file = f"core.{source.lower()}_tables"

    try:
        if source == "twitch":
            games_df = df.filter(df.record_type == "game")
            streams_df = df.filter(df.record_type == "stream")

            if games_df.count() > 0:
                logger.info(f"Cargando {games_df.count()} juegos de Twitch...")
                games_df = (
                    games_df.withColumn("dw_id_carga", lit(None))
                    .withColumn("dw_endpoint", lit(endpoint))
                    .withColumn("dw_source", lit(source_upper))
                )
                games_df = drop_invalid_columns(games_df, logger)
                games_df.write.format("jdbc").option("url", jdbc_uri).option(
                    "dbtable", "core.twitch_games"
                ).option("driver", "org.postgresql.Driver").mode("append").save()

            if streams_df.count() > 0:
                logger.info(f"Cargando {streams_df.count()} streams de Twitch...")
                streams_df = (
                    streams_df.withColumn("dw_id_carga", lit(None))
                    .withColumn("dw_endpoint", lit(endpoint))
                    .withColumn("dw_source", lit(source_upper))
                )
                streams_df = drop_invalid_columns(streams_df, logger)
                streams_df.write.format("jdbc").option("url", jdbc_uri).option(
                    "dbtable", "core.twitch_streams"
                ).option("driver", "org.postgresql.Driver").mode("append").save()

            total_records = games_df.count() + streams_df.count()

        elif source == "showdown_pokedex":
            logger.info(f"Cargando {df.count()} entradas del Pokédex de Showdown...")
            df = drop_invalid_columns(df, logger)
            df.write.format("jdbc").option("url", jdbc_uri).option(
                "dbtable", "core.showdown_pokedex"
            ).option("driver", "org.postgresql.Driver").mode("append").save()
            total_records = df.count()

        elif source == "howlongtobeat":
            logger.info(f"Cargando {df.count()} juegos de HowLongToBeat...")
            df = (
                df.withColumn("dw_id_carga", lit(None))
                .withColumn("dw_endpoint", lit(endpoint))
                .withColumn("dw_source", lit(source_upper))
            )
            df = drop_invalid_columns(df, logger)
            df.write.format("jdbc").option("url", jdbc_uri).option(
                "dbtable", "core.hltb_games"
            ).option("driver", "org.postgresql.Driver").mode("append").save()
            total_records = df.count()

        else:
            raise ValueError(f"Fuente desconocida: {source}")

        end_time = datetime.utcnow()
        return {
            "load_start_time": start_time,
            "load_end_time": end_time,
            "records_count": total_records,
            "source": source_upper,
            "endpoint": endpoint,
            "output_file": output_file,
            "status": "SUCCESS",
        }

    except Exception as e:
        logger.error(f"Error cargando datos de {source}: {e}")
        end_time = datetime.utcnow()
        return {
            "load_start_time": start_time,
            "load_end_time": end_time,
            "records_count": 0,
            "source": source_upper,
            "endpoint": endpoint,
            "output_file": output_file,
            "status": "ERROR",
            "error_message": str(e),
        }
