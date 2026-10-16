# Databricks notebook source
# MAGIC %md
# MAGIC # Absenteeism Fact Transformation
# MAGIC Builds the curated absenteeism fact by integrating current absenteeism
# MAGIC records, detailed medical leave records, and historical absenteeism data.
# MAGIC It standardizes identifiers, dates, ICD codes, duration metrics, employee
# MAGIC relationships, leave status, and analytical partition fields.
# MAGIC
# MAGIC This notebook performs transformation only. It creates the temporary view
# MAGIC `source_fact_absenteeism`; physical Delta persistence is handled by the load
# MAGIC layer.
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
        raise ValueError("Public execution must not publish real health data.")

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


absenteeism_table = qualified_table(source_tables["absenteeism"])
medical_leave_details_table = qualified_table(
    source_tables["medical_leave_details"]
)
historical_absenteeism_table = qualified_table(
    source_tables["historical_absenteeism"]
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
    """Select the first available source column from a public/legacy alias list."""
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
    """Convert blank and textual null markers to actual null values."""
    value = F.trim(F.col(column_name).cast("string"))
    return F.when(
        value.isNull()
        | (value == "")
        | F.upper(value).isin("NULL", "NONE", "N/A", "NA", "VAZIO"),
        F.lit(None),
    ).otherwise(value)


def flexible_date(column_name: str):
    """Parse source values that may already be dates or formatted strings."""
    return F.coalesce(
        F.to_date(F.col(column_name)),
        F.to_date(F.col(column_name).cast("string"), "dd/MM/yyyy"),
        F.to_date(F.col(column_name).cast("string"), "yyyy-MM-dd"),
    )


def flexible_timestamp(column_name: str):
    """Parse source values that may be timestamps, dates, or formatted strings."""
    return F.coalesce(
        F.to_timestamp(F.col(column_name)),
        F.to_timestamp(F.col(column_name).cast("string"), "dd/MM/yyyy HH:mm:ss"),
        F.to_timestamp(F.col(column_name).cast("string"), "yyyy-MM-dd HH:mm:ss"),
        flexible_date(column_name).cast("timestamp"),
    )


def standardize_icd_code(column_name: str):
    """Normalize the first ICD code and preserve a valid category/subcategory."""
    first_code = F.upper(
        F.trim(F.element_at(F.split(F.col(column_name).cast("string"), ","), 1))
    )
    alphanumeric_code = F.regexp_replace(first_code, "[^A-Z0-9]", "")

    return (
        F.when(
            first_code.isNull()
            | (first_code == "")
            | first_code.isin("NULL", "NONE", "N/A", "NA", "VAZIO", "0"),
            F.lit("NO_ICD"),
        )
        .when(
            F.length(alphanumeric_code) >= 4,
            F.concat(
                F.substring(alphanumeric_code, 1, 3),
                F.lit("."),
                F.substring(alphanumeric_code, 4, 4),
            ),
        )
        .when(F.length(alphanumeric_code) == 3, alphanumeric_code)
        .otherwise(F.lit("UNKNOWN"))
    )


def decimal_from_string(column_name: str):
    """Convert locale-formatted numeric strings to double."""
    cleaned_value = F.regexp_replace(
        F.trim(F.col(column_name).cast("string")),
        ",",
        ".",
    )
    return F.expr(f"try_cast(`{column_name}` as double)").when if False else cleaned_value.cast("double")


def duration_hours(column_name: str):
    """Convert HH:mm values or decimal strings to fractional hours."""
    value = F.trim(F.col(column_name).cast("string"))
    return (
        F.when(
            value.contains(":"),
            F.expr(
                f"try_cast(split(`{column_name}`, ':')[0] as double) + "
                f"(try_cast(split(`{column_name}`, ':')[1] as double) / 60.0)"
            ),
        )
        .otherwise(
            F.regexp_replace(value, ",", ".").cast("double")
        )
    )


# COMMAND ----------

# MAGIC %md
# MAGIC ## Read source tables and transformed dimensions

# COMMAND ----------

absenteeism_raw_df = normalize_dataframe_columns(
    spark.read.table(absenteeism_table)
)
medical_leave_details_raw_df = normalize_dataframe_columns(
    spark.read.table(medical_leave_details_table)
)
historical_absenteeism_raw_df = normalize_dataframe_columns(
    spark.read.table(historical_absenteeism_table)
)

if not spark.catalog.tableExists("source_dim_employee"):
    raise RuntimeError(
        "Temporary view 'source_dim_employee' was not found. Run "
        "01_employee_dimension.py in the same Spark session first."
    )

if not spark.catalog.tableExists("source_dim_icd"):
    raise RuntimeError(
        "Temporary view 'source_dim_icd' was not found. Run "
        "02_icd_dimension.py in the same Spark session first."
    )

employee_dimension_df = spark.table("source_dim_employee")
icd_dimension_df = spark.table("source_dim_icd")

if data_quality_config["fail_on_empty_required_source"]:
    if absenteeism_raw_df.limit(1).count() == 0:
        raise ValueError("The current absenteeism source is empty.")

    if historical_absenteeism_raw_df.limit(1).count() == 0:
        raise ValueError("The historical absenteeism source is empty.")

    if employee_dimension_df.limit(1).count() == 0:
        raise ValueError("The employee dimension is empty.")

    if icd_dimension_df.limit(1).count() == 0:
        raise ValueError("The ICD dimension is empty.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Standardize current absenteeism records

# COMMAND ----------

current_absenteeism_df = absenteeism_raw_df.select(
    first_existing_column(
        absenteeism_raw_df,
        ["codigo_sequencial_atestado", "id_atestado", "absence_id"],
        "absence_id",
    ),
    first_existing_column(
        absenteeism_raw_df,
        ["matricula_func", "matricula", "id_matricula", "employee_id"],
        "employee_source_id",
    ),
    first_existing_column(
        absenteeism_raw_df,
        ["cid_principal", "codigo_cid", "id_cid", "icd_code"],
        "raw_icd_code",
        required=False,
    ),
    first_existing_column(
        absenteeism_raw_df,
        ["descricao_cid", "desc_doenca", "diagnosis_description"],
        "diagnosis_description",
        required=False,
    ),
    first_existing_column(
        absenteeism_raw_df,
        ["unidade", "nome_unidade", "unit_name"],
        "unit_name",
        required=False,
    ),
    first_existing_column(
        absenteeism_raw_df,
        ["dt_inicio_atestado", "dt_inicio_afastamento", "absence_start_date"],
        "absence_start_date_raw",
    ),
    first_existing_column(
        absenteeism_raw_df,
        ["dt_fim_atestado", "dt_fim_afastamento", "absence_end_date"],
        "absence_end_date_raw",
        required=False,
    ),
    first_existing_column(
        absenteeism_raw_df,
        ["dt_ficha_atestado", "dt_ficha_afastamento", "record_date"],
        "record_date_raw",
        required=False,
    ),
    first_existing_column(
        absenteeism_raw_df,
        ["dt_criacao", "creation_date", "created_at"],
        "creation_timestamp_raw",
        required=False,
    ),
    first_existing_column(
        absenteeism_raw_df,
        ["hora_inicio_atestado", "start_time"],
        "absence_start_time",
        required=False,
    ),
    first_existing_column(
        absenteeism_raw_df,
        ["hora_fim_atestado", "end_time"],
        "absence_end_time",
        required=False,
    ),
    first_existing_column(
        absenteeism_raw_df,
        ["dias_afastados", "qtd_dias_afastado", "absence_days"],
        "absence_days_raw",
        required=False,
    ),
    first_existing_column(
        absenteeism_raw_df,
        ["horas_afastado", "qtd_horas_afastado", "absence_hours"],
        "absence_hours_raw",
        required=False,
    ),
    first_existing_column(
        absenteeism_raw_df,
        ["tipo_atestado", "certificate_type"],
        "certificate_type",
        required=False,
    ),
    first_existing_column(
        absenteeism_raw_df,
        ["tipo_licenca", "leave_type"],
        "leave_type",
        required=False,
    ),
    first_existing_column(
        absenteeism_raw_df,
        ["motivo_licenca", "desc_motivo_licenca", "leave_reason"],
        "leave_reason",
        required=False,
    ),
).withColumn("data_origin", F.lit("CURRENT_SYSTEM"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Standardize detailed medical leave records

# COMMAND ----------

medical_leave_df = medical_leave_details_raw_df.select(
    first_existing_column(
        medical_leave_details_raw_df,
        ["codigo_sequencial_licenca", "codigosequenciallicenca", "leave_id"],
        "absence_id",
    ),
    first_existing_column(
        medical_leave_details_raw_df,
        ["matricula_func", "matricula", "employee_id", "codigo_funcionario"],
        "employee_source_id",
    ),
    first_existing_column(
        medical_leave_details_raw_df,
        ["cids", "cid", "codigo_cid", "icd_code"],
        "raw_icd_code",
        required=False,
    ),
    first_existing_column(
        medical_leave_details_raw_df,
        ["descricao_motivo", "descricaomotivo", "diagnosis_description"],
        "diagnosis_description",
        required=False,
    ),
    first_existing_column(
        medical_leave_details_raw_df,
        ["nome_unidade", "unidade", "unit_name"],
        "unit_name",
        required=False,
    ),
    first_existing_column(
        medical_leave_details_raw_df,
        ["data_inicio_afastamento", "datainicioafastamento", "absence_start_date"],
        "absence_start_date_raw",
    ),
    first_existing_column(
        medical_leave_details_raw_df,
        ["data_fim_afastamento", "datafimafastamento", "absence_end_date"],
        "absence_end_date_raw",
        required=False,
    ),
    first_existing_column(
        medical_leave_details_raw_df,
        ["data_ficha", "dataficha", "record_date"],
        "record_date_raw",
        required=False,
    ),
    first_existing_column(
        medical_leave_details_raw_df,
        ["data_criacao", "datacriacao", "creation_date"],
        "creation_timestamp_raw",
        required=False,
    ),
    first_existing_column(
        medical_leave_details_raw_df,
        ["hora_inicio_atestado", "hora_inicio", "start_time"],
        "absence_start_time",
        required=False,
    ),
    first_existing_column(
        medical_leave_details_raw_df,
        ["hora_fim_atestado", "hora_fim", "end_time"],
        "absence_end_time",
        required=False,
    ),
    first_existing_column(
        medical_leave_details_raw_df,
        ["dias_afastados", "diasafastados", "absence_days"],
        "absence_days_raw",
        required=False,
    ),
    first_existing_column(
        medical_leave_details_raw_df,
        ["horas_afastado", "horasafastado", "absence_hours"],
        "absence_hours_raw",
        required=False,
    ),
    first_existing_column(
        medical_leave_details_raw_df,
        ["tipo_atestado", "tipoatestado", "certificate_type"],
        "certificate_type",
        required=False,
    ),
    first_existing_column(
        medical_leave_details_raw_df,
        ["tipo_licenca", "tipolicenca", "leave_type"],
        "leave_type",
        required=False,
    ),
    first_existing_column(
        medical_leave_details_raw_df,
        ["descricao_motivo", "descricaomotivo", "leave_reason"],
        "leave_reason",
        required=False,
    ),
).withColumn("data_origin", F.lit("MEDICAL_LEAVE_API"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Standardize historical absenteeism records

# COMMAND ----------

historical_absenteeism_df = historical_absenteeism_raw_df.select(
    first_existing_column(
        historical_absenteeism_raw_df,
        ["id_atestado", "codigo_sequencial_atestado", "absence_id"],
        "source_absence_id",
        required=False,
    ),
    first_existing_column(
        historical_absenteeism_raw_df,
        ["matricula", "employee_id", "id_matricula"],
        "employee_source_id",
    ),
    first_existing_column(
        historical_absenteeism_raw_df,
        ["cid", "codigo_cid", "id_cid", "icd_code"],
        "raw_icd_code",
        required=False,
    ),
    first_existing_column(
        historical_absenteeism_raw_df,
        ["doenca", "descricao_cid", "diagnosis_description"],
        "diagnosis_description",
        required=False,
    ),
    first_existing_column(
        historical_absenteeism_raw_df,
        ["unidade", "nome_unidade", "unit_name"],
        "unit_name",
        required=False,
    ),
    first_existing_column(
        historical_absenteeism_raw_df,
        ["inicio_afast", "dt_inicio_afastamento", "absence_start_date"],
        "absence_start_date_raw",
    ),
    first_existing_column(
        historical_absenteeism_raw_df,
        ["fim_afast", "dt_fim_afastamento", "absence_end_date"],
        "absence_end_date_raw",
        required=False,
    ),
    first_existing_column(
        historical_absenteeism_raw_df,
        ["data_atestado", "dt_ficha_afastamento", "record_date"],
        "record_date_raw",
        required=False,
    ),
    F.lit(None).cast("string").alias("creation_timestamp_raw"),
    F.lit(None).cast("string").alias("absence_start_time"),
    F.lit(None).cast("string").alias("absence_end_time"),
    first_existing_column(
        historical_absenteeism_raw_df,
        ["d_total", "dias_afastados", "absence_days"],
        "absence_days_raw",
        required=False,
    ),
    first_existing_column(
        historical_absenteeism_raw_df,
        ["h_total", "horas_afastado", "absence_hours"],
        "absence_hours_raw",
        required=False,
    ),
    first_existing_column(
        historical_absenteeism_raw_df,
        ["tipo_atestado", "certificate_type"],
        "certificate_type",
        required=False,
    ),
    F.lit(None).cast("string").alias("leave_type"),
    first_existing_column(
        historical_absenteeism_raw_df,
        ["situacao", "motivo_licenca", "leave_reason"],
        "leave_reason",
        required=False,
    ),
).withColumn("data_origin", F.lit("LEGACY_SYSTEM"))

historical_absenteeism_df = historical_absenteeism_df.withColumn(
    "absence_id",
    F.coalesce(
        null_if_blank("source_absence_id"),
        F.sha2(
            F.concat_ws(
                "|",
                F.lit("LEGACY_ABSENCE"),
                null_if_blank("employee_source_id"),
                F.col("absence_start_date_raw"),
                F.coalesce(null_if_blank("raw_icd_code"), F.lit("NO_ICD")),
                F.coalesce(null_if_blank("absence_days_raw"), F.lit("0")),
                F.coalesce(null_if_blank("absence_hours_raw"), F.lit("0")),
            ),
            256,
        ),
    ),
).drop("source_absence_id")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Consolidate all absenteeism sources

# COMMAND ----------

absenteeism_union_df = (
    current_absenteeism_df
    .unionByName(medical_leave_df, allowMissingColumns=True)
    .unionByName(historical_absenteeism_df, allowMissingColumns=True)
)

absenteeism_standardized_df = (
    absenteeism_union_df
    .withColumn("absence_id", null_if_blank("absence_id"))
    .withColumn("employee_source_id", null_if_blank("employee_source_id"))
    .withColumn("icd_code", standardize_icd_code("raw_icd_code"))
    .withColumn("diagnosis_description", null_if_blank("diagnosis_description"))
    .withColumn("unit_name", F.initcap(null_if_blank("unit_name")))
    .withColumn("absence_start_date", flexible_date("absence_start_date_raw"))
    .withColumn("absence_end_date_reported", flexible_date("absence_end_date_raw"))
    .withColumn("record_date", flexible_date("record_date_raw"))
    .withColumn(
        "creation_timestamp",
        F.coalesce(
            flexible_timestamp("creation_timestamp_raw"),
            F.col("record_date").cast("timestamp"),
        ),
    )
    .withColumn("absence_days_reported", decimal_from_string("absence_days_raw"))
    .withColumn("absence_hours_reported", duration_hours("absence_hours_raw"))
    .withColumn("certificate_type", null_if_blank("certificate_type"))
    .withColumn("leave_type", null_if_blank("leave_type"))
    .withColumn("leave_reason", null_if_blank("leave_reason"))
    .drop(
        "raw_icd_code",
        "absence_start_date_raw",
        "absence_end_date_raw",
        "record_date_raw",
        "creation_timestamp_raw",
        "absence_days_raw",
        "absence_hours_raw",
    )
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Calculate standardized duration and final dates

# COMMAND ----------

HOURS_PER_WORKDAY = 8.0

absenteeism_standardized_df = (
    absenteeism_standardized_df
    .withColumn(
        "duration_type",
        F.when(
            F.col("absence_days_reported").isNotNull()
            & (F.col("absence_days_reported") > 0),
            F.lit("DAYS"),
        )
        .when(
            F.col("absence_hours_reported").isNotNull()
            & (F.col("absence_hours_reported") > 0),
            F.lit("HOURS"),
        )
        .otherwise(F.lit("NOT_INFORMED")),
    )
    .withColumn(
        "total_absence_hours",
        F.when(
            F.col("duration_type") == "HOURS",
            F.coalesce(F.col("absence_hours_reported"), F.lit(0.0)),
        )
        .when(
            F.col("duration_type") == "DAYS",
            F.coalesce(
                F.col("absence_days_reported") * F.lit(HOURS_PER_WORKDAY),
                F.lit(0.0),
            ),
        )
        .otherwise(F.lit(0.0)),
    )
    .withColumn(
        "total_absence_days",
        F.when(
            F.col("duration_type") == "DAYS",
            F.coalesce(F.col("absence_days_reported"), F.lit(0.0)),
        )
        .when(
            F.col("duration_type") == "HOURS",
            F.coalesce(
                F.col("absence_hours_reported") / F.lit(HOURS_PER_WORKDAY),
                F.lit(0.0),
            ),
        )
        .otherwise(F.lit(0.0)),
    )
    .withColumn(
        "absence_end_date_calculated",
        F.date_add(
            F.col("absence_start_date"),
            F.greatest(
                F.ceil(F.col("total_absence_days")).cast("int"),
                F.lit(0),
            ),
        ),
    )
    .withColumn(
        "absence_end_date",
        F.coalesce(
            F.col("absence_end_date_reported"),
            F.col("absence_end_date_calculated"),
            F.col("absence_start_date"),
        ),
    )
    .withColumn(
        "absence_status",
        F.when(F.col("absence_end_date") < F.current_date(), F.lit("RETURNED"))
        .otherwise(F.lit("ON_LEAVE")),
    )
    .withColumn(
        "absence_year",
        F.year(F.col("absence_start_date")),
    )
    .drop("absence_end_date_calculated", "absence_end_date_reported")
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Resolve employee analytical keys

# COMMAND ----------

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

absenteeism_enriched_df = absenteeism_standardized_df.join(
    F.broadcast(employee_map_df),
    on="employee_source_id",
    how="left",
)

# Preserve referential integrity through a deterministic unknown employee member.
UNKNOWN_EMPLOYEE_KEY = F.sha2(F.lit("EMPLOYEE|UNKNOWN"), 256)
absenteeism_enriched_df = absenteeism_enriched_df.withColumn(
    "employee_key",
    F.coalesce(F.col("employee_key"), UNKNOWN_EMPLOYEE_KEY),
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Resolve ICD analytical keys

# COMMAND ----------

icd_map_df = icd_dimension_df.select("icd_code", "icd_key").dropDuplicates(
    ["icd_code"]
)

absenteeism_enriched_df = absenteeism_enriched_df.join(
    F.broadcast(icd_map_df),
    on="icd_code",
    how="left",
)

UNKNOWN_ICD_KEY = F.sha2(F.lit("ICD|UNKNOWN"), 256)
absenteeism_enriched_df = absenteeism_enriched_df.withColumn(
    "icd_key",
    F.coalesce(F.col("icd_key"), UNKNOWN_ICD_KEY),
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Generate fact key and deduplicate records

# COMMAND ----------

absenteeism_enriched_df = (
    absenteeism_enriched_df
    .filter(F.col("absence_id").isNotNull())
    .withColumn(
        "absence_key",
        F.sha2(
            F.concat_ws("|", F.lit("ABSENCE"), F.col("absence_id")),
            256,
        ),
    )
    .withColumn("record_type", F.lit("ABSENTEEISM"))
    .withColumn("snapshot_timestamp", F.current_timestamp())
)

source_priority = (
    F.when(F.col("data_origin") == "CURRENT_SYSTEM", F.lit(3))
    .when(F.col("data_origin") == "MEDICAL_LEAVE_API", F.lit(2))
    .otherwise(F.lit(1))
)

record_completeness = sum(
    F.when(F.col(column_name).isNotNull(), F.lit(1)).otherwise(F.lit(0))
    for column_name in [
        "employee_source_id",
        "icd_code",
        "diagnosis_description",
        "unit_name",
        "absence_start_date",
        "absence_end_date",
        "creation_timestamp",
        "certificate_type",
        "leave_type",
        "leave_reason",
    ]
)

absence_dedup_window = Window.partitionBy("absence_key").orderBy(
    source_priority.desc(),
    record_completeness.desc(),
    F.col("creation_timestamp").desc_nulls_last(),
)

absenteeism_fact_df = (
    absenteeism_enriched_df
    .withColumn("row_number", F.row_number().over(absence_dedup_window))
    .filter(F.col("row_number") == 1)
    .drop("row_number")
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Select the curated fact schema

# COMMAND ----------

absenteeism_fact_df = absenteeism_fact_df.select(
    "absence_key",
    "absence_id",
    "employee_key",
    "employee_source_id",
    "icd_key",
    "icd_code",
    "diagnosis_description",
    "unit_name",
    "absence_start_date",
    "absence_end_date",
    "record_date",
    "creation_timestamp",
    "absence_start_time",
    "absence_end_time",
    "absence_days_reported",
    "absence_hours_reported",
    "total_absence_days",
    "total_absence_hours",
    "duration_type",
    "certificate_type",
    "leave_type",
    "leave_reason",
    "record_type",
    "data_origin",
    "absence_year",
    "absence_status",
    "snapshot_timestamp",
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Apply final data-quality checks

# COMMAND ----------

if absenteeism_fact_df.limit(1).count() == 0:
    raise ValueError("The absenteeism fact transformation produced no records.")

invalid_key_count = (
    absenteeism_fact_df
    .filter(
        F.col("absence_key").isNull()
        | F.col("absence_id").isNull()
        | F.col("employee_key").isNull()
        | F.col("icd_key").isNull()
    )
    .limit(1)
    .count()
)

if invalid_key_count > 0:
    raise ValueError("The absenteeism fact contains null analytical keys.")

invalid_date_count = (
    absenteeism_fact_df
    .filter(F.col("absence_start_date").isNull())
    .limit(1)
    .count()
)

if invalid_date_count > 0:
    raise ValueError("The absenteeism fact contains records without a start date.")

negative_duration_count = (
    absenteeism_fact_df
    .filter(
        (F.col("total_absence_days") < 0)
        | (F.col("total_absence_hours") < 0)
    )
    .limit(1)
    .count()
)

if negative_duration_count > 0:
    raise ValueError("The absenteeism fact contains negative duration metrics.")

if data_quality_config["reject_duplicate_business_keys"]:
    duplicate_key_count = (
        absenteeism_fact_df
        .groupBy("absence_key")
        .count()
        .filter(F.col("count") > 1)
        .limit(1)
        .count()
    )

    if duplicate_key_count > 0:
        raise ValueError("The absenteeism fact contains duplicate analytical keys.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Register output view

# COMMAND ----------

OUTPUT_VIEW = "source_fact_absenteeism"
absenteeism_fact_df.createOrReplaceTempView(OUTPUT_VIEW)

record_count = absenteeism_fact_df.count()
print(
    "Absenteeism fact transformation completed successfully. "
    f"Records: {record_count}"
)
print(f"Temporary view created: {OUTPUT_VIEW}")
