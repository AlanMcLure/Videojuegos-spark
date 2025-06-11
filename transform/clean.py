# transform/clean.py
from pyspark.sql import DataFrame
from pyspark.sql.functions import *
from pyspark.sql.types import *
import re


def normalize_column_names(df: DataFrame) -> DataFrame:
    """
    Normalizar nombres de columnas: sustituir espacios por _, quitar acentos, etc.
    """
    new_columns = []
    for col_name in df.columns:
        normalized = col_name.lower()
        normalized = re.sub(r"[^a-z0-9]", "_", normalized)
        normalized = re.sub(r"_+", "_", normalized)
        normalized = normalized.strip("_")
        new_columns.append(normalized)
    for old, new in zip(df.columns, new_columns):
        if old != new:
            df = df.withColumnRenamed(old, new)
    return df


def add_technical_columns(
    df: DataFrame, source: str, endpoint: str, load_id: int = None
) -> DataFrame:
    """
    Añadir columnas técnicas de Data Warehouse (DW)
    """
    return (
        df.withColumn("dw_fecha_registro", current_timestamp())
        .withColumn("dw_id_carga", lit(load_id))
        .withColumn("dw_deleted", lit(False))
        .withColumn("dw_source", lit(source.upper()))
        .withColumn("dw_endpoint", lit(endpoint))
    )


def clean_twitch_data(df: DataFrame, logger) -> DataFrame:
    """
    Limpiar y transformar datos de Twitch (games y streams)
    """
    logger.info("Iniciando limpieza de datos de Twitch...")

    df = normalize_column_names(df)
    logger.info(f"Columnas recibidas: {df.columns}")
    df.show(5, truncate=False)

    initial_count = df.count()

    games_df = df.filter(col("record_type") == "game")
    streams_df = df.filter(col("record_type") == "stream")

    if games_df.rdd.isEmpty() and streams_df.rdd.isEmpty():
        logger.warning("Twitch: no hay datos para limpiar")
        return df

    if not games_df.rdd.isEmpty():
        logger.info("Limpieza específica para twitch_games")
        games_df = games_df.dropDuplicates(["game_id"])

        if "game_name" in games_df.columns:
            games_df = games_df.filter(
                col("game_name").isNotNull() & (col("game_name") != "")
            )
            games_df = games_df.withColumn("game_name", trim(col("game_name")))

        if "box_art_url" in games_df.columns:
            games_df = games_df.withColumn(
                "box_art_url",
                when(
                    col("box_art_url").isNull() | (col("box_art_url") == ""), None
                ).otherwise(col("box_art_url")),
            )

        if "igdb_id" in games_df.columns:
            games_df = games_df.withColumn(
                "igdb_id",
                when(col("igdb_id").isNull() | (col("igdb_id") == ""), None).otherwise(
                    col("igdb_id")
                ),
            )

    if not streams_df.rdd.isEmpty():
        logger.info("Limpieza específica para twitch_streams")
        streams_df = streams_df.dropDuplicates(["stream_id"])

        if "user_name" in streams_df.columns:
            streams_df = streams_df.filter(
                col("user_name").isNotNull() & (col("user_name") != "")
            )

        if "title" in streams_df.columns:
            streams_df = streams_df.withColumn("title", trim(col("title")))

        if "viewer_count" in streams_df.columns:
            streams_df = streams_df.withColumn(
                "viewer_count",
                when(col("viewer_count").isNull(), 0).otherwise(
                    col("viewer_count").cast(IntegerType())
                ),
            )

        if "started_at" in streams_df.columns:
            streams_df = streams_df.withColumn(
                "started_at",
                to_timestamp(col("started_at"), "yyyy-MM-dd'T'HH:mm:ss'Z'"),
            )

        if "extraction_time" in games_df.columns:
            games_df = games_df.withColumn(
                "extraction_time", to_timestamp(col("extraction_time"))
            )

        if "extraction_time" in streams_df.columns:
            streams_df = streams_df.withColumn(
                "extraction_time", to_timestamp(col("extraction_time"))
            )

        if "language" in streams_df.columns:
            streams_df = streams_df.withColumn(
                "language",
                when(
                    col("language").isNull() | (col("language") == ""), "unknown"
                ).otherwise(col("language")),
            )

        if "is_mature" in streams_df.columns:
            streams_df = streams_df.withColumn(
                "is_mature", col("is_mature").cast(BooleanType())
            )

        if (
            "extraction_time" in games_df.columns
            and games_df.schema["extraction_time"].dataType.simpleString() == "string"
        ):
            games_df = games_df.withColumn(
                "extraction_time", to_timestamp("extraction_time")
            )

        if (
            "extraction_time" in streams_df.columns
            and streams_df.schema["extraction_time"].dataType.simpleString() == "string"
        ):
            streams_df = streams_df.withColumn(
                "extraction_time", to_timestamp("extraction_time")
            )

    # Unir ambos DataFrames si hay datos
    clean_df = (
        games_df.unionByName(streams_df, allowMissingColumns=True)
        if not games_df.rdd.isEmpty() and not streams_df.rdd.isEmpty()
        else games_df
        if not games_df.rdd.isEmpty()
        else streams_df
    )

    clean_df = add_technical_columns(clean_df, "TWITCH", "twitch_api")
    clean_df = clean_df.drop("source", "endpoint")
    final_count = clean_df.count()
    logger.info(f"Limpieza Twitch: {initial_count} -> {final_count} registros")
    clean_df.printSchema()

    return clean_df


# def clean_showdown_data(df: DataFrame, logger) -> DataFrame:
#     """
#     Limpiar y transformar datos de Pokémon Showdown (ladder y profiles)
#     """
#     logger.info("Iniciando limpieza de datos de Showdown...")
#     df = normalize_column_names(df)
#     logger.info(f"Columnas recibidas: {df.columns}")
#     df.show(5, truncate=False)
#     initial_count = df.count()

#     # Separar tipos de registros
#     ladder_df = df.filter(col("record_type") == "ladder")
#     profile_df = df.filter(col("record_type") == "profile")

#     if not ladder_df.rdd.isEmpty():
#         logger.info("Limpieza específica para ladder")
#         ladder_df = ladder_df.dropDuplicates(["username"])
#         ladder_df = ladder_df.withColumn("elo", col("elo").cast(DoubleType()))
#         ladder_df = ladder_df.withColumn("rating", col("rating").cast(DoubleType()))
#         for col_name in ["wins", "losses", "games"]:
#             ladder_df = ladder_df.withColumn(
#                 col_name, col(col_name).cast(IntegerType())
#             )

#     if not profile_df.rdd.isEmpty():
#         logger.info("Limpieza específica para profiles")
#         profile_df = profile_df.dropDuplicates(["username"])
#         profile_df = profile_df.withColumn(
#             "games_played", col("games_played").cast(IntegerType())
#         )
#         profile_df = profile_df.withColumn(
#             "win_ratio", col("win_ratio").cast(DoubleType())
#         )

#     clean_df = (
#         ladder_df.unionByName(profile_df, allowMissingColumns=True)
#         if not ladder_df.rdd.isEmpty() and not profile_df.rdd.isEmpty()
#         else ladder_df
#         if not ladder_df.rdd.isEmpty()
#         else profile_df
#     )

#     clean_df = add_technical_columns(clean_df, "SHOWDOWN", "showdown_api")
#     final_count = clean_df.count()
#     logger.info(f"Limpieza Showdown: {initial_count} -> {final_count} registros")
#     return clean_df


def clean_pokedex_data(df: DataFrame, logger) -> DataFrame:
    """
    Limpieza y transformación del Pokédex de Showdown
    """
    logger.info("Iniciando limpieza de datos del Pokédex de Showdown...")
    df = normalize_column_names(df)

    # Convertir extraction_time si está presente
    if "extraction_time" in df.columns:
        df = df.withColumn("extraction_time", to_timestamp(col("extraction_time")))

    logger.info(f"Columnas recibidas: {df.columns}")
    df.show(5, truncate=False)
    initial_count = df.count()

    # Extraer estadísticas base del diccionario baseStats
    for stat in ["hp", "atk", "def", "spa", "spd", "spe"]:
        df = df.withColumn(stat, col("basestats").getItem(stat).cast(IntegerType()))

    # Extraer tipos individuales desde el array "types"
    df = df.withColumn("type1", col("types").getItem(0))

    try:
        df = df.withColumn(
            "type2", try_element_at(col("types"), 2)
        )  # index 2 = segundo tipo
    except:
        logger.warning("type2 falló. No se incluirá esta columna.")
        pass  # No agregamos nada y continuamos

    # Otras columnas pueden mantenerse como están (abilities como map)
    df = df.withColumn("pokemon_id", col("pokemon_id").cast(StringType()))
    df = df.withColumn("name", trim(col("name")))

    # Eliminar duplicados por ID o nombre si fuera necesario
    df = df.dropDuplicates(["pokemon_id"])

    if (
        "extraction_time" in df.columns
        and df.schema["extraction_time"].dataType.simpleString() == "string"
    ):
        df = df.withColumn("extraction_time", to_timestamp("extraction_time"))

    # Añadir columnas técnicas DW
    clean_df = add_technical_columns(df, "SHOWDOWN", "showdown_pokedex")
    clean_df = clean_df.drop("source", "endpoint", "basestats")
    final_count = clean_df.count()
    logger.info(f"Limpieza Pokédex: {initial_count} -> {final_count} registros")
    return clean_df


def clean_hltb_data(df: DataFrame, logger) -> DataFrame:
    """
    Limpiar y transformar datos de HowLongToBeat
    """
    logger.info("Iniciando limpieza de datos de HowLongToBeat...")

    df = normalize_column_names(df)

    # Convertir extraction_time a timestamp si existe
    if "extraction_time" in df.columns:
        df = df.withColumn("extraction_time", to_timestamp(col("extraction_time")))

    logger.info(f"Columnas recibidas: {df.columns}")
    df.show(5, truncate=False)
    initial_count = df.count()

    # Drop duplicates si existe 'game_title'
    if "game_title" in df.columns:
        df = df.dropDuplicates(["game_title"])

    # Filtrado por título no vacío
    if "game_title" in df.columns:
        df = df.filter(col("game_title").isNotNull() & (col("game_title") != ""))
        df = df.withColumn("game_title", trim(col("game_title")))

    # Limpieza de alias
    if "game_alias" in df.columns:
        df = df.withColumn(
            "game_alias",
            when(
                col("game_alias").isNull() | (col("game_alias") == ""), None
            ).otherwise(trim(col("game_alias"))),
        )

    # Tiempos
    for colname in ["main_history", "main_extra", "completionist"]:
        if colname in df.columns:
            df = df.withColumn(
                colname,
                when(col(colname).isNull() | (col(colname) <= 0), None).otherwise(
                    col(colname).cast(DecimalType(6, 2))
                ),
            )

    # Puntuaciones
    if "review_score" in df.columns:
        df = df.withColumn(
            "review_score",
            when(
                col("review_score").isNull() | (col("review_score") <= 0), None
            ).otherwise(col("review_score").cast(DecimalType(3, 1))),
        )

    # Conteos
    for colname in ["count_comp", "count_review"]:
        if colname in df.columns:
            df = df.withColumn(
                colname,
                when(col(colname).isNull(), 0).otherwise(
                    col(colname).cast(IntegerType())
                ),
            )

    # Filtro: al menos uno de los tiempos debe existir
    if all(c in df.columns for c in ["main_history", "main_extra", "completionist"]):
        df = df.filter(
            col("main_history").isNotNull()
            | col("main_extra").isNotNull()
            | col("completionist").isNotNull()
        )

    if (
        "extraction_time" in df.columns
        and df.schema["extraction_time"].dataType.simpleString() == "string"
    ):
        df = df.withColumn("extraction_time", to_timestamp("extraction_time"))

    clean_df = add_technical_columns(df, "HLTB", "howlongtobeat_api")
    clean_df = clean_df.drop("source", "endpoint")
    final_count = clean_df.count()
    logger.info(f"Limpieza HLTB: {initial_count} -> {final_count} registros")

    return clean_df


def clean_and_transform_data(df: DataFrame, source: str, logger) -> DataFrame:
    logger.info(f"Iniciando limpieza para fuente: {source}")
    source = source.lower()

    if source == "twitch_games" or source == "twitch_streams":
        return clean_twitch_data(df, logger)
    elif source == "howlongtobeat":
        return clean_hltb_data(df, logger)
    elif source == "showdown_pokedex":
        return clean_pokedex_data(df, logger)
    else:
        raise ValueError(f"Fuente no soportada: {source}")


def validate_data_quality(df: DataFrame, source: str, logger) -> dict:
    """
    Validar calidad de los datos después de la limpieza:
      - total_records
      - porcentaje de nulos por columna
      - conteo de duplicados en clave primaria
      - reporte de columnas vacías
    """
    logger.info(f"Validando calidad de datos para {source}")
    total = df.count()
    nulls = {}
    for c in df.columns:
        null_count = df.filter(col(c).isNull() | (col(c) == "")).count()
        nulls[c] = round((null_count / total * 100), 2) if total else None
    pk = df.columns[0] if df.columns else None
    dup = df.count() - df.dropDuplicates([pk]).count() if pk else None
    empty_cols = [c for c, pct in nulls.items() if pct and pct > 0]
    passed = (dup == 0) and all(pct == 0 for pct in nulls.values() if pct is not None)
    report = {
        "source": source,
        "total_records": total,
        "null_percentage": nulls,
        "duplicate_count": dup,
        "columns_with_empty": empty_cols,
        "validation_passed": passed,
    }
    logger.info(f"Data quality report: {report}")
    return report
