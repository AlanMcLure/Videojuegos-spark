# tests/test_postgres_write.py

import logging
from sqlalchemy import create_engine, text
from datetime import datetime
import os

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Coge la URI del entorno o la pones manual si quieres
DB_URI = os.getenv(
    "VG_CORE_DB_URI", "postgresql+psycopg2://vg_etl_user:YourStrongPass@db:5432/vg_core"
)


def test_insert_and_read():
    logger.info("🔍 Iniciando test de escritura y lectura en la tabla core.hltb_games")

    try:
        engine = create_engine(DB_URI)
        with engine.connect() as conn:
            # Insertar un registro de prueba
            insert_query = text("""
                INSERT INTO core.hltb_games (
                    game_title, comp_main, comp_plus, comp_100, extraction_time,
                    dw_id_carga, dw_source, dw_endpoint
                ) VALUES (
                    :game_title, :comp_main, :comp_plus, :comp_100, :extraction_time,
                    :dw_id_carga, :dw_source, :dw_endpoint
                )
                RETURNING id;
            """)

            values = {
                "game_title": "Juego de prueba",
                "comp_main": 10.5,
                "comp_plus": 15.0,
                "comp_100": 25.0,
                "extraction_time": datetime.utcnow(),
                "dw_id_carga": 1,  # Usa un log de prueba existente
                "dw_source": "HLTB",
                "dw_endpoint": "test/manual_insert",
            }

            result = conn.execute(insert_query, values)
            inserted_id = result.scalar()
            logger.info(f"✅ Registro insertado con ID: {inserted_id}")

            # Leer el registro insertado
            read_query = text("SELECT * FROM core.hltb_games WHERE id = :id")
            read_result = conn.execute(read_query, {"id": inserted_id}).fetchone()
            logger.info(f"📄 Registro recuperado: {read_result}")

    except Exception as e:
        logger.error(f"❌ Error durante el test: {str(e)}")


if __name__ == "__main__":
    test_insert_and_read()
