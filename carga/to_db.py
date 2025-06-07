# load/to_db.py
from sqlalchemy import (
    create_engine,
    MetaData,
    Table,
    Column,
    Integer,
    String,
    DateTime,
    Boolean,
    Text,
    DECIMAL,
    ARRAY,
)
from sqlalchemy.sql import text
from datetime import datetime
import logging


def init_log_table(db_uri: str):
    """
    Inicializar conexión a la base de datos y tabla de logs
    """
    try:
        engine = create_engine(db_uri)

        # Probar conexión
        with engine.connect() as conn:
            result = conn.execute(text("SELECT 1"))

        # Definir tabla de logs usando SQLAlchemy Core
        metadata = MetaData()
        logs_table = Table(
            "etl_logs",
            metadata,
            Column("id", Integer, primary_key=True, autoincrement=True),
            Column("load_start_time", DateTime(timezone=True), nullable=False),
            Column("load_end_time", DateTime(timezone=True), nullable=False),
            Column("records_count", Integer, nullable=False, default=0),
            Column("source", String(50), nullable=False),
            Column("endpoint", String(200)),
            Column("output_file", String(200)),
            Column("status", String(20), default="SUCCESS"),
            Column("error_message", Text),
            Column("created_at", DateTime(timezone=True), default=datetime.utcnow),
            schema="logs",
        )

        return engine, logs_table

    except Exception as e:
        logging.error(f"Error inicializando base de datos: {str(e)}")
        raise


def insert_log(engine, logs_table, log_data: dict):
    """
    Insertar registro en la tabla de logs
    """
    try:
        # Preparar datos del log
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
            # Insertar registro
            result = conn.execute(logs_table.insert().values(**log_record))
            conn.commit()

            # Obtener ID del registro insertado
            log_id = result.inserted_primary_key[0]

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
        # Determinar tabla destino según la fuente
        if source.lower() == "twitch":
            # Separar games y streams
            games_df = df.filter(df.record_type == "game")
            streams_df = df.filter(df.record_type == "stream")

            # Cargar games
            if games_df.count() > 0:
                logger.info(f"Cargando {games_df.count()} juegos de Twitch...")
                games_df.write.format("jdbc").option(
                    "url", engine.url.render_as_string(hide_password=False)
                ).option("dbtable", "core.twitch_games").option(
                    "user", engine.url.username
                ).option("password", engine.url.password).option(
                    "driver", "org.postgresql.Driver"
                ).mode("append").save()

            # Cargar streams
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
            # Separar battles y pokemon usage
            battles_df = df.filter(df.record_type == "battle")
            usage_df = df.filter(df.record_type == "pokemon_usage")

            if battles_df.count() > 0:
                logger.info(f"Cargando {battles_df.count()} batallas de Showdown...")
                battles_df.write.format("jdbc").option(
                    "url", engine.url.render_as_string(hide_password=False)
                ).option("dbtable", "core.showdown_battles").option(
                    "user", engine.url.username
                ).option("password", engine.url.password).option(
                    "driver", "org.postgresql.Driver"
                ).mode("append").save()

            if usage_df.count() > 0:
                logger.info(
                    f"Cargando {usage_df.count()} estadísticas de uso de Pokémon..."
                )
                usage_df.write.format("jdbc").option(
                    "url", engine.url.render_as_string(hide_password=False)
                ).option("dbtable", "core.showdown_pokemon_usage").option(
                    "user", engine.url.username
                ).option("password", engine.url.password).option(
                    "driver", "org.postgresql.Driver"
                ).mode("append").save()

            total_records = battles_df.count() + usage_df.count()

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

        # Crear metadata para el log
        load_metadata = {
            "load_start_time": start_time,
            "load_end_time": end_time,
            "records_count": total_records,
            "source": source.upper(),
            "endpoint": f"load/core_{source.lower()}",
            "output_file": f"core.{source.lower()}_tables",
            "status": "SUCCESS",
        }

        logger.info(
            f"Carga a BD completada: {total_records} registros en {(end_time - start_time).total_seconds():.2f} segundos"
        )

        return load_metadata

    except Exception as e:
        end_time = datetime.utcnow()
        error_metadata = {
            "load_start_time": start_time,
            "load_end_time": end_time,
            "records_count": 0,
            "source": source.upper(),
            "endpoint": f"load/core_{source.lower()}",
            "output_file": f"core.{source.lower()}_tables",
            "status": "ERROR",
            "error_message": str(e),
        }

        logger.error(f"Error cargando {source} a BD: {str(e)}")
        return error_metadata


def execute_sql_file(engine, sql_file_path: str):
    """
    Ejecutar archivo SQL para crear estructura de base de datos
    """
    try:
        with open(sql_file_path, "r", encoding="utf-8") as file:
            sql_content = file.read()

        with engine.connect() as conn:
            # Dividir por comandos individuales
            commands = sql_content.split(";")

            for command in commands:
                command = command.strip()
                if command and not command.startswith("--"):
                    conn.execute(text(command))

            conn.commit()

        logging.info(f"Archivo SQL ejecutado correctamente: {sql_file_path}")

    except Exception as e:
        logging.error(f"Error ejecutando archivo SQL {sql_file_path}: {str(e)}")
        raise


def get_database_stats(engine):
    """
    Obtener estadísticas de la base de datos
    """
    try:
        stats_query = """
        SELECT 
            schemaname,
            tablename,
            n_tup_ins as total_inserts,
            n_tup_upd as total_updates,
            n_tup_del as total_deletes,
            n_live_tup as live_tuples,
            n_dead_tup as dead_tuples,
            last_vacuum,
            last_autovacuum,
            last_analyze,
            last_autoanalyze
        FROM pg_stat_user_tables 
        WHERE schemaname IN ('core', 'logs')
        ORDER BY schemaname, tablename;
        """

        with engine.connect() as conn:
            result = conn.execute(text(stats_query))
            stats = result.fetchall()

        return stats

    except Exception as e:
        logging.error(f"Error obteniendo estadísticas de BD: {str(e)}")
        return []


def cleanup_old_logs(engine, days_to_keep: int = 30):
    """
    Limpiar logs antiguos de la tabla de logs
    """
    try:
        cleanup_query = (
            """
        DELETE FROM logs.etl_logs 
        WHERE created_at < NOW() - INTERVAL '%s days'
        """
            % days_to_keep
        )

        with engine.connect() as conn:
            result = conn.execute(text(cleanup_query))
            deleted_count = result.rowcount
            conn.commit()

        logging.info(f"Limpieza completada: {deleted_count} logs antiguos eliminados")
        return deleted_count

    except Exception as e:
        logging.error(f"Error en limpieza de logs: {str(e)}")
        raise
