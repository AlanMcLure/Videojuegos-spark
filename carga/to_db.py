from sqlalchemy import create_engine, text
from datetime import datetime
import logging


def init_log_table(db_uri: str):
    """
    Inicializar conexión a la base de datos
    """
    try:
        engine = create_engine(db_uri)

        # Probar conexión
        with engine.connect() as conn:
            conn.execute(text("SELECT 1"))

        return engine

    except Exception as e:
        logging.error(f"Error inicializando base de datos: {str(e)}")
        raise


def insert_log(engine, log_data: dict):
    """
    Insertar registro en la tabla de logs
    """
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
            log_id = result.scalar()

        logging.info(f"Log insertado con ID: {log_id}")
        return log_id

    except Exception as e:
        logging.error(f"Error insertando log: {str(e)}")
        raise


def load_to_core_schema(df, source: str, engine, logger):
    """
    Cargar DataFrame limpio al schema core de la base de datos
    """
    start_time = datetime.utcnow()

    try:
        total_records = 0

        if source.lower() == "twitch":
            games_df = df.filter(df.record_type == "game")
            streams_df = df.filter(df.record_type == "stream")

            if games_df.count() > 0:
                logger.info(f"Cargando {games_df.count()} juegos de Twitch...")
                games_df.write.format("jdbc").option(
                    "url", engine.url.render_as_string(hide_password=False)
                ).option("dbtable", "core.twitch_games").option(
                    "user", engine.url.username
                ).option("password", engine.url.password).option(
                    "driver", "org.postgresql.Driver"
                ).mode("append").save()

            if streams_df.count() > 0:
                logger.info(f"Cargando {streams_df.count()} streams de Twitch...")
                streams_df.write.format("jdbc").option(
                    "url", engine.url.render_as_string(hide_password=False)
                ).option("dbtable", "core.twitch_streams").option(
                    "user", engine.url.username
                ).option("password", engine.url.password).option(
                    "driver", "org.postgresql.Driver"
                ).mode("append").save()

            total_records = games_df.count() + streams_df.count()

        elif source.lower() == "showdown":
            ladder_df = df.filter(df.record_type == "ladder")
            profiles_df = df.filter(df.record_type == "profile")

            if ladder_df.count() > 0:
                logger.info(
                    f"Cargando {ladder_df.count()} registros de ladder de Showdown..."
                )
                ladder_df.write.format("jdbc").option(
                    "url", engine.url.render_as_string(hide_password=False)
                ).option("dbtable", "core.showdown_ladder").option(
                    "user", engine.url.username
                ).option("password", engine.url.password).option(
                    "driver", "org.postgresql.Driver"
                ).mode("append").save()

            if profiles_df.count() > 0:
                logger.info(f"Cargando {profiles_df.count()} perfiles de Showdown...")
                profiles_df.write.format("jdbc").option(
                    "url", engine.url.render_as_string(hide_password=False)
                ).option("dbtable", "core.showdown_profiles").option(
                    "user", engine.url.username
                ).option("password", engine.url.password).option(
                    "driver", "org.postgresql.Driver"
                ).mode("append").save()

            total_records = ladder_df.count() + profiles_df.count()

        elif source.lower() == "howlongtobeat":
            logger.info(f"Cargando {df.count()} juegos de HowLongToBeat...")
            df.write.format("jdbc").option(
                "url", engine.url.render_as_string(hide_password=False)
            ).option("dbtable", "core.hltb_games").option(
                "user", engine.url.username
            ).option("password", engine.url.password).option(
                "driver", "org.postgresql.Driver"
            ).mode("append").save()

            total_records = df.count()

        else:
            raise ValueError(f"Fuente desconocida: {source}")

        end_time = datetime.utcnow()

        return {
            "load_start_time": start_time,
            "load_end_time": end_time,
            "records_count": total_records,
            "source": source.upper(),
            "endpoint": f"load/core_{source.lower()}",
            "output_file": f"core.{source.lower()}_tables",
            "status": "SUCCESS",
        }

    except Exception as e:
        end_time = datetime.utcnow()
        return {
            "load_start_time": start_time,
            "load_end_time": end_time,
            "records_count": 0,
            "source": source.upper(),
            "endpoint": f"load/core_{source.lower()}",
            "output_file": f"core.{source.lower()}_tables",
            "status": "ERROR",
            "error_message": str(e),
        }
