# Databricks notebook source
# MAGIC %md
# MAGIC # Occupational Accident Fact Transformation
# MAGIC Builds the curated occupational accident fact by standardizing accident
# MAGIC records, resolving employee relationships, normalizing dates and flags,
# MAGIC generating stable analytical keys, and preparing the partition year.
# MAGIC
# MAGIC This notebook performs transformation only. It creates the temporary view
# MAGIC `source_fact_occupational_accident`; physical Delta persistence is handled
# MAGIC by the load layer.
# MAGIC
# MAGIC The public portfolio version uses generic table and column names and must be
# MAGIC executed only with synthetic, anonymized, or properly governed source data.

# COMMAND ----------

import json
import re
from pathlib import Path
from typing import Dict, Iterable

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window

# COMMAND ----------

# MAGIC %md
# MAGIC ## Load and validate configuration

# COMMAND ----------

CONFIG_PATH = "../../config/config.example.json"


def load_config(config_path: str) -> dict:
    """Load repository configuration and enforce privacy-safe defaults."""
    path = Path(config_path)

    if not path.exists():
        raise FileNotFoundError(f"Configuration file not found: {config_path}")

    with path.open("r", encoding="utf-8") as config_file:
        config = json.load(config_file)

    security = config.get("security", {})
    privacy = config.get("privacy", {})

    if security.get("allow_plaintext_secrets", True):
        raise ValueError("Plaintext secrets must remain disabled.")

    if privacy.get("publish_real_data", True):
        raise ValueError("Public execution must not publish real personal data.")

    return config


config = load_config(CONFIG_PATH)
storage_config = config["storage"]
source_tables = config["tables"]["sources"]
data_quality_config = config["data_quality"]

catalog = storage_config["catalog"]
schema = storage_config["schema"]


def qualified_table(table_name: str) -> str:
    """Return a three-part catalog.schema.table identifier."""
    return f"{catalog}.{schema}.{table_name}"


occupational_accident_table = qualified_table(
    source_tables["occupational_accident"]
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Shared transformation helpers

# COMMAND ----------


def sanitize_column_name(column_name: str) -> str:
    """Convert a source column name to lowercase snake_case."""
    normalized = re.sub(r"[^A-Za-z0-9]+", "_", column_name.strip())
    normalized = re.sub(r"_+", "_", normalized).strip("_")
    return normalized.lower()


def normalize_dataframe_columns(dataframe: DataFrame) -> DataFrame:
    """Normalize source column names while rejecting naming collisions."""
    renamed_columns: Dict[str, str] = {
        current: sanitize_column_name(current) for current in dataframe.columns
    }

    normalized_names = list(renamed_columns.values())
    collisions = sorted(
        name for name in set(normalized_names) if normalized_names.count(name) > 1
    )

    if collisions:
        raise ValueError(
            "Column-name normalization produced collisions: "
            + ", ".join(collisions)
        )

    result = dataframe
    for current_name, normalized_name in renamed_columns.items():
        result = result.withColumnRenamed(current_name, normalized_name)

    return result


def first_existing_column(
    dataframe: DataFrame,
    candidates: Iterable[str],
    output_name: str,
    required: bool = True,
    data_type: str = "string",
):
    """Select the first available source column from an alias list."""
    for candidate in candidates:
        if candidate in dataframe.columns:
            return F.col(candidate).cast(data_type).alias(output_name)

    if required and data_quality_config["fail_on_schema_mismatch"]:
        raise ValueError(
            f"No source column was found for '{output_name}'. "
            f"Expected one of: {', '.join(candidates)}"
        )

    return F.lit(None).cast(data_type).alias(output_name)


def null_if_blank(column_name: str):
    """Convert blanks and textual null markers to actual null values."""
    value = F.trim(F.col(column_name).cast("string"))
    return F.when(
        value.isNull()
        | (value == "")
        | F.upper(value).isin("NULL", "NONE", "N/A", "NA", "VAZIO"),
        F.lit(None),
    ).otherwise(value)


def flexible_date(column_name: str):
    """Parse values that may already be dates or formatted strings."""
    return F.coalesce(
        F.to_date(F.col(column_name)),
        F.to_date(F.col(column_name).cast("string"), "dd/MM/yyyy"),
        F.to_date(F.col(column_name).cast("string"), "yyyy-MM-dd"),
    )


def flexible_timestamp(column_name: str):
    """Parse values that may be timestamps, dates, or formatted strings."""
    return F.coalesce(
        F.to_timestamp(F.col(column_name)),
        F.to_timestamp(F.col(column_name).cast("string"), "dd/MM/yyyy HH:mm:ss"),
        F.to_timestamp(F.col(column_name).cast("string"), "yyyy-MM-dd HH:mm:ss"),
        flexible_date(column_name).cast("timestamp"),
    )


def normalized_boolean(column_name: str):
    """Normalize common boolean representations to a nullable boolean."""
    value = F.upper(F.trim(F.col(column_name).cast("string")))
    return (
        F.when(value.isin("1", "TRUE", "T", "YES", "Y", "SIM", "S"), F.lit(True))
        .when(value.isin("0", "FALSE", "F", "NO", "N", "NAO", "NÃO"), F.lit(False))
        .otherwise(F.lit(None).cast("boolean"))
    )


def non_negative_integer(column_name: str):
    """Convert a numeric source field to a nullable non-negative integer."""
    numeric_value = F.regexp_replace(
        F.trim(F.col(column_name).cast("string")),
        ",",
        ".",
    ).cast("double")

    return F.when(numeric_value >= 0, F.floor(numeric_value).cast("int")).otherwise(
        F.lit(None).cast("int")
    )


# COMMAND ----------

# MAGIC %md
# MAGIC ## Read source table and employee dimension

# COMMAND ----------

occupational_accident_raw_df = normalize_dataframe_columns(
    spark.read.table(occupational_accident_table)
)

if not spark.catalog.tableExists("source_dim_employee"):
    raise RuntimeError(
        "Temporary view 'source_dim_employee' was not found. Run "
        "01_employee_dimension.py in the same Spark session first."
    )

employee_dimension_df = spark.table("source_dim_employee")

if data_quality_config["fail_on_empty_required_source"]:
    if occupational_accident_raw_df.limit(1).count() == 0:
        raise ValueError("The occupational accident source is empty.")

    if employee_dimension_df.limit(1).count() == 0:
        raise ValueError("The employee dimension is empty.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Standardize occupational accident records

# COMMAND ----------

occupational_accident_df = occupational_accident_raw_df.select(
    first_existing_column(
        occupational_accident_raw_df,
        ["numerocat", "numero_cat", "id_cat", "accident_id"],
        "accident_id",
    ),
    first_existing_column(
        occupational_accident_raw_df,
        ["cpf", "tax_id", "person_id", "num_cpf"],
        "source_person_id",
        required=False,
    ),
    first_existing_column(
        occupational_accident_raw_df,
        ["matricula", "employee_id", "id_matricula"],
        "employee_source_id",
        required=False,
    ),
    first_existing_column(
        occupational_accident_raw_df,
        ["datadoacidente", "data_do_acidente", "dt_acidente", "accident_date"],
        "accident_date_raw",
    ),
    first_existing_column(
        occupational_accident_raw_df,
        ["datadoregistro", "data_do_registro", "dt_registro", "record_date"],
        "record_timestamp_raw",
        required=False,
    ),
    first_existing_column(
        occupational_accident_raw_df,
        [
            "ultimodiatrabalho",
            "ultimo_dia_trabalho",
            "dt_ultimo_dia_trabalho",
            "last_workday_date",
        ],
        "last_workday_date_raw",
        required=False,
    ),
    first_existing_column(
        occupational_accident_raw_df,
        ["diasafastado", "dias_afastado", "qtd_dias_afastado", "absence_days"],
        "absence_days_raw",
        required=False,
    ),
    first_existing_column(
        occupational_accident_raw_df,
        ["afastamento", "flg_afastamento", "leave_flag"],
        "leave_flag_raw",
        required=False,
    ),
    first_existing_column(
        occupational_accident_raw_df,
        ["morte", "flg_morte", "fatality_flag"],
        "fatality_flag_raw",
        required=False,
    ),
    first_existing_column(
        occupational_accident_raw_df,
        ["tipodecat", "tipo_de_cat", "tipo_cat", "cat_type"],
        "cat_type",
        required=False,
    ),
    first_existing_column(
        occupational_accident_raw_df,
        ["tipodeacidente", "tipo_de_acidente", "tipo_acidente", "accident_type"],
        "accident_type",
        required=False,
    ),
    first_existing_column(
        occupational_accident_raw_df,
        ["tipo", "tipo_registro", "record_type"],
        "record_type",
        required=False,
    ),
    first_existing_column(
        occupational_accident_raw_df,
        [
            "partedocorpoatingido",
            "parte_do_corpo_atingido",
            "desc_parte_corpo",
            "affected_body_part",
        ],
        "affected_body_part",
        required=False,
    ),
    first_existing_column(
        occupational_accident_raw_df,
        ["agentecausador", "agente_causador", "desc_agente_causador", "causing_agent"],
        "causing_agent",
        required=False,
    ),
    first_existing_column(
        occupational_accident_raw_df,
        ["arvoresdecausa", "arvores_de_causa", "desc_arvore_causa", "cause_tree"],
        "cause_tree",
        required=False,
    ),
    first_existing_column(
        occupational_accident_raw_df,
        ["causasdoacidente", "causas_do_acidente", "desc_causa_acidente", "accident_cause"],
        "accident_cause",
        required=False,
    ),
    first_existing_column(
        occupational_accident_raw_df,
        [
            "situacaogeradora",
            "situacao_geradora",
            "desc_situacao_geradora",
            "generating_situation",
        ],
        "generating_situation",
        required=False,
    ),
    first_existing_column(
        occupational_accident_raw_df,
        [
            "atividadenoatodoacidente",
            "atividade_no_ato_do_acidente",
            "desc_atividade_momento",
            "activity_at_accident",
        ],
        "activity_at_accident",
        required=False,
    ),
    first_existing_column(
        occupational_accident_raw_df,
        ["localdoacidente", "local_do_acidente", "desc_local_acidente", "accident_location"],
        "accident_location",
        required=False,
    ),
    first_existing_column(
        occupational_accident_raw_df,
        [
            "potencialdeacidente",
            "potencial_de_acidente",
            "desc_potencial_acidente",
            "accident_potential",
        ],
        "accident_potential",
        required=False,
    ),
    first_existing_column(
        occupational_accident_raw_df,
        ["area", "nome_area", "area_name"],
        "area_name",
        required=False,
    ),
    first_existing_column(
        occupational_accident_raw_df,
        ["emitente", "nome_emitente", "issuer_name"],
        "issuer_name",
        required=False,
    ),
    first_existing_column(
        occupational_accident_raw_df,
        [
            "ocorrenciadoregistro",
            "ocorrencia_do_registro",
            "desc_ocorrencia",
            "record_occurrence",
        ],
        "record_occurrence",
        required=False,
    ),
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Apply field-level transformations

# COMMAND ----------

TEXT_COLUMNS = [
    "cat_type",
    "accident_type",
    "record_type",
    "affected_body_part",
    "causing_agent",
    "cause_tree",
    "accident_cause",
    "generating_situation",
    "activity_at_accident",
    "accident_location",
    "accident_potential",
    "area_name",
    "issuer_name",
    "record_occurrence",
]

occupational_accident_df = (
    occupational_accident_df
    .withColumn("accident_id", null_if_blank("accident_id"))
    .withColumn("source_person_id", null_if_blank("source_person_id"))
    .withColumn("employee_source_id", null_if_blank("employee_source_id"))
    .withColumn("accident_date", flexible_date("accident_date_raw"))
    .withColumn("record_timestamp", flexible_timestamp("record_timestamp_raw"))
    .withColumn("last_workday_date", flexible_date("last_workday_date_raw"))
    .withColumn("absence_days", non_negative_integer("absence_days_raw"))
    .withColumn("leave_flag", normalized_boolean("leave_flag_raw"))
    .withColumn("fatality_flag", normalized_boolean("fatality_flag_raw"))
)

for column_name in TEXT_COLUMNS:
    occupational_accident_df = occupational_accident_df.withColumn(
        column_name,
        null_if_blank(column_name),
    )

occupational_accident_df = occupational_accident_df.drop(
    "accident_date_raw",
    "record_timestamp_raw",
    "last_workday_date_raw",
    "absence_days_raw",
    "leave_flag_raw",
    "fatality_flag_raw",
)

occupational_accident_df = (
    occupational_accident_df
    .withColumn(
        "record_type",
        F.coalesce(F.upper(F.col("record_type")), F.lit("OCCUPATIONAL_ACCIDENT")),
    )
    .withColumn(
        "accident_status",
        F.when(F.col("fatality_flag") == F.lit(True), F.lit("FATAL"))
        .when(
            (F.col("leave_flag") == F.lit(True))
            | (F.coalesce(F.col("absence_days"), F.lit(0)) > 0),
            F.lit("WITH_LEAVE"),
        )
        .otherwise(F.lit("WITHOUT_LEAVE")),
    )
    .withColumn("accident_year", F.year(F.col("accident_date")))
    .withColumn("data_origin", F.lit("OCCUPATIONAL_ACCIDENT_SOURCE"))
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Resolve employee analytical keys

# COMMAND ----------

# The governed dimension does not expose the original person identifier, so the
# primary public relationship uses employee registration identifiers. A temporary
# deterministic person key is used only when the source does not contain a usable
# employee registration and cannot be matched to the public dimension.
employee_hr_map_df = (
    employee_dimension_df
    .select(
        F.col("employee_hr_id").cast("string").alias("employee_source_id"),
        "employee_key",
    )
    .filter(F.col("employee_source_id").isNotNull())
)

employee_registration_map_df = (
    employee_dimension_df
    .select(
        F.col("employee_registration_id")
        .cast("string")
        .alias("employee_source_id"),
        "employee_key",
    )
    .filter(F.col("employee_source_id").isNotNull())
)

employee_map_df = (
    employee_hr_map_df
    .unionByName(employee_registration_map_df)
    .withColumn("employee_source_id", F.trim(F.col("employee_source_id")))
    .filter(F.col("employee_source_id") != "")
    .dropDuplicates(["employee_source_id"])
)

occupational_accident_df = occupational_accident_df.join(
    F.broadcast(employee_map_df),
    on="employee_source_id",
    how="left",
)

UNKNOWN_EMPLOYEE_KEY = F.sha2(F.lit("EMPLOYEE|UNKNOWN"), 256)
occupational_accident_df = occupational_accident_df.withColumn(
    "employee_key",
    F.coalesce(F.col("employee_key"), UNKNOWN_EMPLOYEE_KEY),
)

# The raw personal identifier is deliberately removed from the curated fact.
occupational_accident_df = occupational_accident_df.drop("source_person_id")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Generate stable fact key and deduplicate records

# COMMAND ----------

occupational_accident_df = occupational_accident_df.withColumn(
    "accident_business_id",
    F.coalesce(
        F.col("accident_id"),
        F.sha2(
            F.concat_ws(
                "|",
                F.lit("ACCIDENT_SOURCE"),
                F.coalesce(F.col("employee_source_id"), F.lit("UNKNOWN_EMPLOYEE")),
                F.col("accident_date").cast("string"),
                F.coalesce(F.col("accident_type"), F.lit("UNKNOWN_TYPE")),
                F.coalesce(F.col("accident_location"), F.lit("UNKNOWN_LOCATION")),
            ),
            256,
        ),
    ),
)

occupational_accident_df = (
    occupational_accident_df
    .withColumn(
        "accident_key",
        F.sha2(
            F.concat_ws(
                "|",
                F.lit("OCCUPATIONAL_ACCIDENT"),
                F.col("accident_business_id"),
            ),
            256,
        ),
    )
    .withColumn("snapshot_timestamp", F.current_timestamp())
)

record_completeness = sum(
    F.when(F.col(column_name).isNotNull(), F.lit(1)).otherwise(F.lit(0))
    for column_name in [
        "employee_source_id",
        "accident_date",
        "record_timestamp",
        "last_workday_date",
        "absence_days",
        "cat_type",
        "accident_type",
        "affected_body_part",
        "causing_agent",
        "accident_cause",
        "generating_situation",
        "accident_location",
        "area_name",
    ]
)

accident_dedup_window = Window.partitionBy("accident_key").orderBy(
    record_completeness.desc(),
    F.col("record_timestamp").desc_nulls_last(),
)

occupational_accident_fact_df = (
    occupational_accident_df
    .withColumn("row_number", F.row_number().over(accident_dedup_window))
    .filter(F.col("row_number") == 1)
    .drop("row_number")
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Select the curated fact schema

# COMMAND ----------

occupational_accident_fact_df = occupational_accident_fact_df.select(
    "accident_key",
    F.col("accident_business_id").alias("accident_id"),
    "employee_key",
    "employee_source_id",
    "accident_date",
    "record_timestamp",
    "last_workday_date",
    "absence_days",
    "leave_flag",
    "fatality_flag",
    "accident_status",
    "cat_type",
    "accident_type",
    "record_type",
    "affected_body_part",
    "causing_agent",
    "cause_tree",
    "accident_cause",
    "generating_situation",
    "activity_at_accident",
    "accident_location",
    "accident_potential",
    "area_name",
    "issuer_name",
    "record_occurrence",
    "data_origin",
    "accident_year",
    "snapshot_timestamp",
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Apply final data-quality checks

# COMMAND ----------

if occupational_accident_fact_df.limit(1).count() == 0:
    raise ValueError(
        "The occupational accident fact transformation produced no records."
    )

invalid_key_count = (
    occupational_accident_fact_df
    .filter(
        F.col("accident_key").isNull()
        | F.col("accident_id").isNull()
        | F.col("employee_key").isNull()
    )
    .limit(1)
    .count()
)

if invalid_key_count > 0:
    raise ValueError(
        "The occupational accident fact contains null analytical keys."
    )

invalid_date_count = (
    occupational_accident_fact_df
    .filter(F.col("accident_date").isNull())
    .limit(1)
    .count()
)

if invalid_date_count > 0:
    raise ValueError(
        "The occupational accident fact contains records without an accident date."
    )

negative_duration_count = (
    occupational_accident_fact_df
    .filter(F.col("absence_days") < 0)
    .limit(1)
    .count()
)

if negative_duration_count > 0:
    raise ValueError(
        "The occupational accident fact contains negative absence durations."
    )

if data_quality_config["reject_duplicate_business_keys"]:
    duplicate_key_count = (
        occupational_accident_fact_df
        .groupBy("accident_key")
        .count()
        .filter(F.col("count") > 1)
        .limit(1)
        .count()
    )

    if duplicate_key_count > 0:
        raise ValueError(
            "The occupational accident fact contains duplicate analytical keys."
        )

# COMMAND ----------

# MAGIC %md
# MAGIC ## Register output view

# COMMAND ----------

OUTPUT_VIEW = "source_fact_occupational_accident"
occupational_accident_fact_df.createOrReplaceTempView(OUTPUT_VIEW)

record_count = occupational_accident_fact_df.count()
print(
    "Occupational accident fact transformation completed successfully. "
    f"Records: {record_count}"
)
print(f"Temporary view created: {OUTPUT_VIEW}")
