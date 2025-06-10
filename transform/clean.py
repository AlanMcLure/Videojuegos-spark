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

    # Unir ambos DataFrames si hay datos
    clean_df = (
        games_df.unionByName(streams_df, allowMissingColumns=True)
        if not games_df.rdd.isEmpty() and not streams_df.rdd.isEmpty()
        else games_df
        if not games_df.rdd.isEmpty()
        else streams_df
    )

    clean_df = add_technical_columns(clean_df, "TWITCH", "twitch_api")
    final_count = clean_df.count()
    logger.info(f"Limpieza Twitch: {initial_count} -> {final_count} registros")

    return clean_df


def clean_showdown_data(df: DataFrame, logger) -> DataFrame:
    """
    Limpiar y transformar datos de Pokémon Showdown (tipos: ladder y profile)
    """
    logger.info("Iniciando limpieza de datos de Showdown...")
    df = normalize_column_names(df)
    logger.info(f"Columnas recibidas: {df.columns}")
    df.show(5, truncate=False)
    initial_count = df.count()

    ladder_df = df.filter(col("record_type") == "ladder")
    profile_df = df.filter(col("record_type") == "profile")

    if not ladder_df.rdd.isEmpty():
        ladder_df = (
            ladder_df.dropDuplicates(["username", "format"])
            .filter(col("username").isNotNull() & (col("username") != ""))
            .withColumn(
                "format",
                when(col("format").isNull(), "unknown").otherwise(trim(col("format"))),
            )
            .withColumn("gxe", col("gxe").cast(DoubleType()))
            .withColumn("rating", col("rating").cast(DoubleType()))
            .withColumn("pr", col("pr").cast(DoubleType()))
        )

    if not profile_df.rdd.isEmpty():
        profile_df = (
            profile_df.dropDuplicates(["username", "format"])
            .filter(col("username").isNotNull() & (col("username") != ""))
            .withColumn(
                "format",
                when(col("format").isNull(), "unknown").otherwise(trim(col("format"))),
            )
            .withColumn("gxe", col("gxe").cast(DoubleType()))
            .withColumn("rating", col("rating").cast(DoubleType()))
            .withColumn(
                "levels", col("levels").cast(MapType(StringType(), IntegerType()))
            )
        )

    clean_df = (
        ladder_df.unionByName(profile_df, allowMissingColumns=True)
        if not ladder_df.rdd.isEmpty() and not profile_df.rdd.isEmpty()
        else ladder_df
        if not ladder_df.rdd.isEmpty()
        else profile_df
    )

    clean_df = add_technical_columns(clean_df, "SHOWDOWN", "showdown_api")
    final_count = clean_df.count()
    logger.info(f"Limpieza Showdown: {initial_count} -> {final_count} registros")
    return clean_df


def clean_hltb_data(df: DataFrame, logger) -> DataFrame:
    """
    Limpiar y transformar datos de HowLongToBeat
    """
    logger.info("Iniciando limpieza de datos de HowLongToBeat...")

    df = normalize_column_names(df)
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
    for colname in ["comp_main", "comp_plus", "comp_100"]:
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
    if all(c in df.columns for c in ["comp_main", "comp_plus", "comp_100"]):
        df = df.filter(
            col("comp_main").isNotNull()
            | col("comp_plus").isNotNull()
            | col("comp_100").isNotNull()
        )

    clean_df = add_technical_columns(df, "HLTB", "howlongtobeat_api")
    final_count = clean_df.count()
    logger.info(f"Limpieza HLTB: {initial_count} -> {final_count} registros")

    return clean_df


def clean_and_transform_data(df: DataFrame, source: str, logger) -> DataFrame:
    """
    Función principal de limpieza según la fuente de datos
    """
    logger.info(f"Iniciando limpieza para fuente: {source}")
    if source.lower() == "twitch":
        return clean_twitch_data(df, logger)
    elif source.lower() == "showdown":
        return clean_showdown_data(df, logger)
    elif source.lower() == "howlongtobeat":
        return clean_hltb_data(df, logger)
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
