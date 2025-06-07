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
    Limpiar y transformar datos de Twitch
    """
    logger.info("Iniciando limpieza de datos de Twitch...")
    df = normalize_column_names(df)
    initial_count = df.count()

    games_df = df.filter(col("record_type") == "game")
    streams_df = df.filter(col("record_type") == "stream")

    if games_df.rdd.isEmpty() and streams_df.rdd.isEmpty():
        logger.warning("Twitch: no hay datos para limpiar")
        return df

    if not games_df.rdd.isEmpty():
        games_df = (
            games_df.dropDuplicates(["id"])
            .filter(col("name").isNotNull() & (col("name") != ""))
            .withColumn("name", trim(col("name")))
            .withColumn(
                "box_art_url",
                when(
                    col("box_art_url").isNull() | (col("box_art_url") == ""), None
                ).otherwise(col("box_art_url")),
            )
            .withColumn(
                "igdb_id",
                when(col("igdb_id").isNull() | (col("igdb_id") == ""), None).otherwise(
                    col("igdb_id")
                ),
            )
        )
    if not streams_df.rdd.isEmpty():
        streams_df = (
            streams_df.dropDuplicates(["stream_id"])
            .filter(col("user_name").isNotNull() & (col("user_name") != ""))
            .withColumn("title", trim(col("title")))
            .withColumn(
                "viewer_count",
                when(col("viewer_count").isNull(), 0).otherwise(
                    col("viewer_count").cast(IntegerType())
                ),
            )
            .withColumn(
                "started_at",
                to_timestamp(col("started_at"), "yyyy-MM-dd'T'HH:mm:ss'Z'"),
            )
            .withColumn(
                "language",
                when(
                    col("language").isNull() | (col("language") == ""), "unknown"
                ).otherwise(col("language")),
            )
            .withColumn("is_mature", col("is_mature").cast(BooleanType()))
        )
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
    Limpiar y transformar datos de Pokémon Showdown
    """
    logger.info("Iniciando limpieza de datos de Showdown...")
    df = normalize_column_names(df)
    initial_count = df.count()

    battles_df = df.filter(col("record_type") == "battle")
    usage_df = df.filter(col("record_type") == "pokemon_usage")

    if not battles_df.rdd.isEmpty():
        battles_df = (
            battles_df.dropDuplicates(["battle_id"])
            .filter(col("battle_id").isNotNull() & (col("battle_id") != ""))
            .withColumn(
                "format",
                when(col("format").isNull(), "unknown").otherwise(trim(col("format"))),
            )
            .withColumn("player1_rating", col("player1_rating").cast(IntegerType()))
            .withColumn("player2_rating", col("player2_rating").cast(IntegerType()))
            .withColumn(
                "turns_count",
                when(col("turns_count").isNull(), 0).otherwise(
                    col("turns_count").cast(IntegerType())
                ),
            )
            .withColumn(
                "duration_seconds",
                when(col("duration_seconds").isNull(), 0).otherwise(
                    col("duration_seconds").cast(IntegerType())
                ),
            )
        )
    if not usage_df.rdd.isEmpty():
        usage_df = (
            usage_df.dropDuplicates(["pokemon_name", "format", "month_year"])
            .filter(col("pokemon_name").isNotNull() & (col("pokemon_name") != ""))
            .withColumn("pokemon_name", trim(col("pokemon_name")))
            .withColumn(
                "format",
                when(col("format").isNull(), "unknown").otherwise(trim(col("format"))),
            )
            .withColumn(
                "usage_percentage", col("usage_percentage").cast(DecimalType(5, 2))
            )
            .withColumn(
                "raw_usage_percentage",
                col("raw_usage_percentage").cast(DecimalType(5, 2)),
            )
            .withColumn(
                "real_usage_percentage",
                col("real_usage_percentage").cast(DecimalType(5, 2)),
            )
            .filter(col("usage_percentage") > 0)
        )
    clean_df = (
        battles_df.unionByName(usage_df, allowMissingColumns=True)
        if not battles_df.rdd.isEmpty() and not usage_df.rdd.isEmpty()
        else battles_df
        if not battles_df.rdd.isEmpty()
        else usage_df
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
    initial_count = df.count()

    clean_df = (
        df.dropDuplicates(["game_id"])
        .filter(col("game_title").isNotNull() & (col("game_title") != ""))
        .withColumn("game_title", trim(col("game_title")))
        .withColumn(
            "game_alias",
            when(
                col("game_alias").isNull() | (col("game_alias") == ""), None
            ).otherwise(trim(col("game_alias"))),
        )
        .withColumn(
            "comp_main",
            when(col("comp_main").isNull() | (col("comp_main") <= 0), None).otherwise(
                col("comp_main").cast(DecimalType(6, 2))
            ),
        )
        .withColumn(
            "comp_plus",
            when(col("comp_plus").isNull() | (col("comp_plus") <= 0), None).otherwise(
                col("comp_plus").cast(DecimalType(6, 2))
            ),
        )
        .withColumn(
            "comp_100",
            when(col("comp_100").isNull() | (col("comp_100") <= 0), None).otherwise(
                col("comp_100").cast(DecimalType(6, 2))
            ),
        )
        .withColumn(
            "review_score",
            when(
                col("review_score").isNull() | (col("review_score") <= 0), None
            ).otherwise(col("review_score").cast(DecimalType(3, 1))),
        )
        .withColumn(
            "count_comp",
            when(col("count_comp").isNull(), 0).otherwise(
                col("count_comp").cast(IntegerType())
            ),
        )
        .withColumn(
            "count_review",
            when(col("count_review").isNull(), 0).otherwise(
                col("count_review").cast(IntegerType())
            ),
        )
    )
    clean_df = clean_df.filter(
        col("comp_main").isNotNull()
        | col("comp_plus").isNotNull()
        | col("comp_100").isNotNull()
    )
    clean_df = add_technical_columns(clean_df, "HLTB", "howlongtobeat_api")
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
