# Databricks notebook source
# MAGIC %md
# MAGIC # Employee Dimension Transformation
# MAGIC Builds the curated employee dimension by integrating the current employee
# MAGIC master, employee disability attributes, historical employee records, and
# MAGIC organizational perimeter mappings.
# MAGIC
# MAGIC This notebook performs transformation only. It creates the temporary view
# MAGIC `source_dim_employee`; physical Delta persistence is handled by the load layer.
# MAGIC
# MAGIC The public portfolio version uses generic table and column names and must be
# MAGIC executed only with synthetic, anonymized, or properly governed source data.

# COMMAND ----------

import json
import re
import unicodedata
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
    """Load the repository configuration and enforce privacy-safe defaults."""
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


employee_master_table = qualified_table(source_tables["employee_master"])
employee_disability_table = qualified_table(source_tables["employee_disability"])
historical_absenteeism_table = qualified_table(
    source_tables["historical_absenteeism"]
)
organizational_perimeter_table = qualified_table(
    source_tables["organizational_perimeter"]
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Shared validation helpers

# COMMAND ----------


def require_columns(
    dataframe: DataFrame,
    required_columns: Iterable[str],
    dataframe_name: str,
) -> None:
    """Fail when a required source column is absent."""
    missing_columns = sorted(set(required_columns) - set(dataframe.columns))

    if missing_columns and data_quality_config["fail_on_schema_mismatch"]:
        raise ValueError(
            f"{dataframe_name} is missing required columns: "
            + ", ".join(missing_columns)
        )


def first_existing_column(
    dataframe: DataFrame,
    candidates: Iterable[str],
    output_name: str,
    required: bool = True,
):
    """Select the first available source column from a public/legacy alias list."""
    for candidate in candidates:
        if candidate in dataframe.columns:
            return F.col(candidate).alias(output_name)

    if required and data_quality_config["fail_on_schema_mismatch"]:
        raise ValueError(
            f"No source column was found for '{output_name}'. "
            f"Expected one of: {', '.join(candidates)}"
        )

    return F.lit(None).alias(output_name)


def normalized_string(column_name: str):
    """Trim a column after casting it to string."""
    return F.trim(F.col(column_name).cast("string"))


def null_if_blank(column_name: str):
    """Convert blanks and textual null markers to actual null values."""
    value = normalized_string(column_name)
    return F.when(
        value.isNull()
        | (value == "")
        | F.upper(value).isin("NULL", "NONE", "N/A", "NA", "VAZIO"),
        F.lit(None),
    ).otherwise(value)


def sanitize_column_name(column_name: str) -> str:
    """Convert accented or punctuated source names to lowercase snake_case."""
    ascii_name = (
        unicodedata.normalize("NFKD", column_name.strip())
        .encode("ascii", "ignore")
        .decode("ascii")
    )
    normalized = re.sub(r"[^A-Za-z0-9]+", "_", ascii_name)
    normalized = re.sub(r"_+", "_", normalized).strip("_")
    return normalized.lower()


def normalize_dataframe_columns(dataframe: DataFrame) -> DataFrame:
    """Normalize source column names while rejecting collisions."""
    renamed_columns: Dict[str, str] = {
        current: sanitize_column_name(current) for current in dataframe.columns
    }

    normalized_names = list(renamed_columns.values())
    duplicates = sorted(
        name for name in set(normalized_names) if normalized_names.count(name) > 1
    )

    if duplicates:
        raise ValueError(
            "Column-name normalization produced collisions: "
            + ", ".join(duplicates)
        )

    result = dataframe
    for current_name, normalized_name in renamed_columns.items():
        result = result.withColumnRenamed(current_name, normalized_name)

    return result


# COMMAND ----------

# MAGIC %md
# MAGIC ## Read source tables

# COMMAND ----------

employee_master_raw_df = normalize_dataframe_columns(
    spark.read.table(employee_master_table)
)

employee_disability_raw_df = normalize_dataframe_columns(
    spark.read.table(employee_disability_table)
)

historical_employee_raw_df = normalize_dataframe_columns(
    spark.read.table(historical_absenteeism_table)
)

organizational_perimeter_raw_df = normalize_dataframe_columns(
    spark.read.table(organizational_perimeter_table)
)

if data_quality_config["fail_on_empty_required_source"]:
    if employee_master_raw_df.limit(1).count() == 0:
        raise ValueError("The employee master source is empty.")

    if historical_employee_raw_df.limit(1).count() == 0:
        raise ValueError("The historical absenteeism source is empty.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Standardize current employee records

# COMMAND ----------

current_employee_df = employee_master_raw_df.select(
    first_existing_column(
        employee_master_raw_df,
        ["matricularh", "matricula_rh", "employee_id", "id_matricula_rh"],
        "employee_hr_id",
    ),
    first_existing_column(
        employee_master_raw_df,
        [
            "matriculafuncionario",
            "matricula_funcionario",
            "employee_registration",
            "id_matricula_funcionario",
        ],
        "employee_registration_id",
        required=False,
    ),
    first_existing_column(
        employee_master_raw_df,
        ["cpf", "tax_id", "person_id", "num_cpf"],
        "source_person_id",
    ),
    first_existing_column(
        employee_master_raw_df,
        ["nome", "nomefuncionario", "nome_funcionario", "employee_name"],
        "employee_name",
    ),
    first_existing_column(
        employee_master_raw_df,
        ["nomecargo", "nome_cargo", "cargo", "job_title"],
        "job_title",
        required=False,
    ),
    first_existing_column(
        employee_master_raw_df,
        [
            "rhcentrocustounidade",
            "ccusto",
            "cod_centro_custo",
            "cost_center_id",
        ],
        "cost_center_id",
        required=False,
    ),
    first_existing_column(
        employee_master_raw_df,
        ["nomeunidade", "nome_unidade", "unidade", "unit_name"],
        "unit_name",
        required=False,
    ),
    first_existing_column(
        employee_master_raw_df,
        ["nomesetor", "nome_setor", "setor", "department_name"],
        "department_name",
        required=False,
    ),
    first_existing_column(
        employee_master_raw_df,
        ["sexo", "gender", "desc_sexo"],
        "gender",
        required=False,
    ),
    first_existing_column(
        employee_master_raw_df,
        ["idade", "age", "num_idade"],
        "age",
        required=False,
    ),
).withColumn("data_origin", F.lit("CURRENT_SYSTEM"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Standardize historical employee records

# COMMAND ----------

historical_employee_df = historical_employee_raw_df.select(
    first_existing_column(
        historical_employee_raw_df,
        ["matricula", "employee_id", "id_matricula", "matricularh"],
        "employee_hr_id",
    ),
    F.lit(None).cast("string").alias("employee_registration_id"),
    first_existing_column(
        historical_employee_raw_df,
        ["colaborador", "employee_name", "nome"],
        "employee_name",
    ),
    first_existing_column(
        historical_employee_raw_df,
        ["cargo", "job_title", "nome_cargo"],
        "job_title",
        required=False,
    ),
    first_existing_column(
        historical_employee_raw_df,
        ["ccusto", "cost_center_id", "cod_centro_custo"],
        "cost_center_id",
        required=False,
    ),
    first_existing_column(
        historical_employee_raw_df,
        ["unidade", "unit_name", "nome_unidade"],
        "unit_name",
        required=False,
    ),
    first_existing_column(
        historical_employee_raw_df,
        ["setor", "department_name", "nome_setor"],
        "department_name",
        required=False,
    ),
    first_existing_column(
        historical_employee_raw_df,
        ["sexo", "gender", "desc_sexo"],
        "gender",
        required=False,
    ),
    first_existing_column(
        historical_employee_raw_df,
        ["idade", "age", "num_idade"],
        "age",
        required=False,
    ),
).filter(
    F.col("employee_hr_id").isNotNull()
    & (F.trim(F.col("employee_hr_id").cast("string")) != "")
)

historical_employee_df = (
    historical_employee_df
    .withColumn("employee_hr_id", normalized_string("employee_hr_id"))
    .withColumn(
        "source_person_id",
        F.concat(F.lit("HIST_"), F.col("employee_hr_id")),
    )
    .withColumn("data_origin", F.lit("LEGACY_SYSTEM"))
)

# Retain only employees not present in the current employee source.
current_employee_ids_df = (
    current_employee_df
    .select(normalized_string("employee_hr_id").alias("employee_hr_id"))
    .filter(
        F.col("employee_hr_id").isNotNull()
        & (F.col("employee_hr_id") != "")
    )
    .dropDuplicates(["employee_hr_id"])
)

historical_only_df = historical_employee_df.join(
    current_employee_ids_df,
    on="employee_hr_id",
    how="left_anti",
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Consolidate current and historical employees

# COMMAND ----------

employee_base_df = current_employee_df.unionByName(
    historical_only_df,
    allowMissingColumns=True,
)

employee_base_df = (
    employee_base_df
    .withColumn("employee_hr_id", null_if_blank("employee_hr_id"))
    .withColumn(
        "employee_registration_id",
        null_if_blank("employee_registration_id"),
    )
    .withColumn("source_person_id", null_if_blank("source_person_id"))
    .withColumn("employee_name", null_if_blank("employee_name"))
    .withColumn("job_title", null_if_blank("job_title"))
    .withColumn("cost_center_id", null_if_blank("cost_center_id"))
    .withColumn("unit_name", null_if_blank("unit_name"))
    .withColumn("department_name", null_if_blank("department_name"))
    .withColumn("gender", null_if_blank("gender"))
    .withColumn("age", F.col("age").cast("int"))
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Standardize organizational perimeter mapping

# COMMAND ----------

perimeter_df = organizational_perimeter_raw_df.select(
    first_existing_column(
        organizational_perimeter_raw_df,
        ["ccusto", "cost_center_id", "cod_centro_custo"],
        "cost_center_id",
    ),
    first_existing_column(
        organizational_perimeter_raw_df,
        ["perimetro", "perimeter_name", "nome_perimetro", "business_perimeter"],
        "perimeter_name",
    ),
)

perimeter_df = (
    perimeter_df
    .withColumn("cost_center_id", null_if_blank("cost_center_id"))
    .withColumn("perimeter_name", null_if_blank("perimeter_name"))
    .filter(F.col("cost_center_id").isNotNull())
    .dropDuplicates(["cost_center_id"])
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Standardize employee disability attributes

# COMMAND ----------

employee_disability_df = employee_disability_raw_df.select(
    first_existing_column(
        employee_disability_raw_df,
        ["matricula", "employee_id", "employee_registration_id"],
        "disability_employee_id",
    ),
    first_existing_column(
        employee_disability_raw_df,
        ["deficiencia", "disability_description"],
        "disability_description",
        required=False,
    ),
    first_existing_column(
        employee_disability_raw_df,
        ["classificacao", "disability_classification"],
        "disability_classification",
        required=False,
    ),
)

employee_disability_df = (
    employee_disability_df
    .withColumn(
        "disability_employee_id",
        null_if_blank("disability_employee_id"),
    )
    .withColumn(
        "disability_description",
        null_if_blank("disability_description"),
    )
    .withColumn(
        "disability_classification",
        null_if_blank("disability_classification"),
    )
    .filter(F.col("disability_employee_id").isNotNull())
)

# Prefer the most complete record when more than one disability row exists.
disability_completeness = (
    F.when(F.col("disability_description").isNotNull(), F.lit(1)).otherwise(F.lit(0))
    + F.when(F.col("disability_classification").isNotNull(), F.lit(1)).otherwise(F.lit(0))
)

disability_window = Window.partitionBy("disability_employee_id").orderBy(
    disability_completeness.desc(),
    F.col("disability_classification").asc_nulls_last(),
    F.col("disability_description").asc_nulls_last(),
)

employee_disability_df = (
    employee_disability_df
    .withColumn("row_number", F.row_number().over(disability_window))
    .filter(F.col("row_number") == 1)
    .drop("row_number")
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Enrich the employee dimension

# COMMAND ----------

employee_enriched_df = (
    employee_base_df
    .join(perimeter_df, on="cost_center_id", how="left")
    .join(
        employee_disability_df,
        F.coalesce(
            F.col("employee_registration_id"),
            F.col("employee_hr_id"),
        )
        == F.col("disability_employee_id"),
        how="left",
    )
    .drop("disability_employee_id")
)

employee_enriched_df = (
    employee_enriched_df
    .withColumn(
        "gender",
        F.when(F.upper(F.trim(F.col("gender"))).isin("1", "M", "MALE", "MASCULINO"), F.lit("M"))
        .when(F.upper(F.trim(F.col("gender"))).isin("2", "F", "FEMALE", "FEMININO"), F.lit("F"))
        .otherwise(F.lit("NOT_INFORMED")),
    )
    .withColumn(
        "age_group",
        F.when(F.col("age").isNull(), F.lit("NOT_INFORMED"))
        .when(F.col("age") < 0, F.lit("NOT_INFORMED"))
        .when(F.col("age") < 20, F.lit("UP_TO_19"))
        .when(F.col("age").between(20, 29), F.lit("20_TO_29"))
        .when(F.col("age").between(30, 39), F.lit("30_TO_39"))
        .when(F.col("age").between(40, 49), F.lit("40_TO_49"))
        .when(F.col("age").between(50, 59), F.lit("50_TO_59"))
        .otherwise(F.lit("60_OR_MORE")),
    )
    .withColumn(
        "disability_description",
        F.coalesce(
            null_if_blank("disability_description"),
            F.lit("NO_DISABILITY_REPORTED"),
        ),
    )
    .withColumn(
        "disability_classification",
        F.coalesce(
            F.regexp_replace(
                null_if_blank("disability_classification"),
                r"^[^A-Za-z0-9]{0,4}",
                "",
            ),
            F.lit("NO_DISABILITY_REPORTED"),
        ),
    )
    .withColumn(
        "unit_name",
        F.initcap(F.trim(F.col("unit_name"))),
    )
    .withColumn(
        "employee_name",
        F.upper(F.trim(F.col("employee_name"))),
    )
    .withColumn(
        "job_title",
        F.initcap(F.trim(F.col("job_title"))),
    )
    .withColumn(
        "department_name",
        F.initcap(F.trim(F.col("department_name"))),
    )
    .withColumn("snapshot_timestamp", F.current_timestamp())
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Generate stable analytical keys

# COMMAND ----------

# The natural person identifier is retained only in the governed transformation
# layer. The final analytical key is a deterministic SHA-256 hash and does not use
# the execution timestamp, ensuring stability between pipeline runs.
employee_enriched_df = employee_enriched_df.withColumn(
    "employee_natural_key",
    F.coalesce(
        null_if_blank("source_person_id"),
        null_if_blank("employee_hr_id"),
        null_if_blank("employee_registration_id"),
    ),
)

employee_enriched_df = employee_enriched_df.withColumn(
    "employee_key",
    F.sha2(
        F.concat_ws(
            "|",
            F.lit("EMPLOYEE"),
            F.col("employee_natural_key"),
        ),
        256,
    ),
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Deduplicate the dimension

# COMMAND ----------

record_completeness = sum(
    F.when(F.col(column_name).isNotNull(), F.lit(1)).otherwise(F.lit(0))
    for column_name in [
        "employee_name",
        "job_title",
        "cost_center_id",
        "perimeter_name",
        "unit_name",
        "department_name",
        "gender",
        "age",
        "disability_description",
    ]
)

employee_dedup_window = Window.partitionBy("employee_natural_key").orderBy(
    F.when(F.col("data_origin") == "CURRENT_SYSTEM", F.lit(1)).otherwise(F.lit(0)).desc(),
    record_completeness.desc(),
    F.col("employee_hr_id").desc_nulls_last(),
)

employee_dimension_df = (
    employee_enriched_df
    .filter(F.col("employee_natural_key").isNotNull())
    .withColumn("row_number", F.row_number().over(employee_dedup_window))
    .filter(F.col("row_number") == 1)
    .drop("row_number")
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Select the curated dimension schema

# COMMAND ----------

employee_dimension_df = employee_dimension_df.select(
    "employee_key",
    "employee_hr_id",
    "employee_registration_id",
    "employee_name",
    "job_title",
    "cost_center_id",
    "perimeter_name",
    "unit_name",
    "department_name",
    "gender",
    "age",
    "age_group",
    "disability_description",
    "disability_classification",
    "data_origin",
    "snapshot_timestamp",
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Apply final data-quality checks

# COMMAND ----------

if employee_dimension_df.limit(1).count() == 0:
    raise ValueError("The employee dimension transformation produced no records.")

null_key_count = (
    employee_dimension_df
    .filter(F.col("employee_key").isNull())
    .limit(1)
    .count()
)

if null_key_count > 0:
    raise ValueError("The employee dimension contains null surrogate keys.")

if data_quality_config["reject_duplicate_business_keys"]:
    duplicate_key_count = (
        employee_dimension_df
        .groupBy("employee_key")
        .count()
        .filter(F.col("count") > 1)
        .limit(1)
        .count()
    )

    if duplicate_key_count > 0:
        raise ValueError("The employee dimension contains duplicate surrogate keys.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Register output view

# COMMAND ----------

OUTPUT_VIEW = "source_dim_employee"
employee_dimension_df.createOrReplaceTempView(OUTPUT_VIEW)

record_count = employee_dimension_df.count()
print(
    "Employee dimension transformation completed successfully. "
    f"Records: {record_count}"
)
print(f"Temporary view created: {OUTPUT_VIEW}")
