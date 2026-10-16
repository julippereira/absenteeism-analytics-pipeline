# Databricks notebook source
# MAGIC %md
# MAGIC # Employee Disability Ingestion
# MAGIC Extracts employee disability and rehabilitation data from the configured
# MAGIC API, applies minimum source-layer standardization, removes duplicate source
# MAGIC records, and writes the result to a Delta source table.
# MAGIC
# MAGIC This public portfolio version contains no credentials, internal endpoints,
# MAGIC company identifiers, or real employee health data.

# COMMAND ----------

import json
from pathlib import Path

import requests
from pyspark.sql import DataFrame
from pyspark.sql.functions import (
    col,
    current_timestamp,
    lit,
    to_date,
    trim,
    upper,
    when,
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Load public configuration

# COMMAND ----------

CONFIG_PATH = "../../config/config.example.json"


def load_config(config_path: str) -> dict:
    """Load repository configuration and enforce secure defaults."""
    path = Path(config_path)

    if not path.exists():
        raise FileNotFoundError(f"Configuration file not found: {config_path}")

    with path.open("r", encoding="utf-8") as config_file:
        config = json.load(config_file)

    security = config.get("security", {})

    if security.get("allow_plaintext_secrets", True):
        raise ValueError("Plaintext secrets must remain disabled.")

    if not security.get("tls_verification", False):
        raise ValueError("TLS verification must remain enabled.")

    return config


config = load_config(CONFIG_PATH)
extraction_config = config["extractions"]["employee_disability"]
security_config = config["security"]
api_config = config["api"]
storage_config = config["storage"]
data_quality_config = config["data_quality"]

if not extraction_config.get("enabled", False):
    raise RuntimeError(
        "Employee disability extraction is disabled in the configuration."
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## Resolve secrets securely

# COMMAND ----------


def get_secret(secret_key: str) -> str:
    """Retrieve a value from Databricks Secrets without logging it."""
    try:
        value = dbutils.secrets.get(
            scope=security_config["secret_scope"],
            key=secret_key,
        )
    except Exception as exc:
        raise RuntimeError(
            f"Required secret could not be retrieved for key: {secret_key}"
        ) from exc

    if not value or not value.strip():
        raise ValueError(f"Secret is empty for key: {secret_key}")

    return value.strip()


api_url = get_secret(api_config["base_url_secret_key"])
company_id = get_secret(api_config["company_id_secret_key"])
report_code = get_secret(extraction_config["report_code_secret_key"])
api_key = get_secret(extraction_config["api_key_secret_key"])

# COMMAND ----------

# MAGIC %md
# MAGIC ## Build target table name

# COMMAND ----------

catalog = storage_config["catalog"]
schema = storage_config["schema"]
target_table = f"{catalog}.{schema}.{extraction_config['target_table']}"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Request employee disability data

# COMMAND ----------

request_parameters = {
    "empresa": company_id,
    "codigo": report_code,
    "chave": api_key,
    "tipoSaida": api_config["output_format"],
}

# Sensitive parameters and response payloads must never be printed.
response = requests.post(
    api_url,
    data={"parametro": json.dumps(request_parameters)},
    timeout=api_config["timeout_seconds"],
    verify=api_config["verify_ssl"],
)
response.raise_for_status()

try:
    response_data = response.json()
except requests.exceptions.JSONDecodeError as exc:
    raise ValueError("The API returned an invalid JSON response.") from exc

if isinstance(response_data, dict):
    response_data = [response_data]

if not isinstance(response_data, list):
    raise TypeError("The API response must be a JSON object or list of objects.")

if not response_data:
    if data_quality_config["fail_on_empty_required_source"]:
        raise ValueError("The employee disability API returned no records.")

    dbutils.notebook.exit(
        "No employee disability records returned. Target table unchanged."
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create and validate source DataFrame

# COMMAND ----------

employee_disability_raw_df = spark.createDataFrame(response_data)

required_columns = {
    "CODFUNCIONARIO",
    "MATRICULAFUNCIONARIO",
    "NOMEFUNCIONARIO",
    "CODIGOTIPODEFICIENCIA",
    "DEFICIENCIA",
    "CLASSIFICACAO",
    "REABILITADO",
    "DATAINICIODEFICIENCIA",
    "DATAREABILITACAO",
}

missing_columns = sorted(
    required_columns - set(employee_disability_raw_df.columns)
)

if missing_columns and data_quality_config["fail_on_schema_mismatch"]:
    raise ValueError(
        "Required source columns are missing: " + ", ".join(missing_columns)
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## Apply source-layer transformations

# COMMAND ----------

employee_disability_df: DataFrame = (
    employee_disability_raw_df
    .select(
        col("CODFUNCIONARIO").cast("string").alias("ID_FUNCIONARIO"),
        col("MATRICULAFUNCIONARIO").cast("string").alias("MATRICULA"),
        when(
            col("NOMEFUNCIONARIO").isNull()
            | (trim(col("NOMEFUNCIONARIO")) == ""),
            lit("NAO INFORMADO"),
        )
        .otherwise(upper(trim(col("NOMEFUNCIONARIO"))))
        .alias("NOME_FUNCIONARIO"),
        col("CODIGOTIPODEFICIENCIA")
        .cast("string")
        .alias("ID_TIPO_DEFICIENCIA"),
        when(
            col("DEFICIENCIA").isNull()
            | (trim(col("DEFICIENCIA")) == ""),
            lit("Nao Informada"),
        )
        .otherwise(trim(col("DEFICIENCIA")))
        .alias("DEFICIENCIA"),
        when(
            col("CLASSIFICACAO").isNull()
            | (trim(col("CLASSIFICACAO")) == ""),
            lit("Nao Classificada"),
        )
        .otherwise(trim(col("CLASSIFICACAO")))
        .alias("CLASSIFICACAO"),
        when(
            col("REABILITADO").isNull()
            | (trim(col("REABILITADO")) == ""),
            lit("Nao"),
        )
        .otherwise(trim(col("REABILITADO")))
        .alias("REABILITADO"),
        to_date(col("DATAINICIODEFICIENCIA"), "dd/MM/yyyy").alias(
            "DATA_INICIO_DEFICIENCIA"
        ),
        to_date(col("DATAREABILITACAO"), "dd/MM/yyyy").alias(
            "DATA_REABILITACAO"
        ),
    )
    .filter(
        col("ID_FUNCIONARIO").isNotNull()
        & (trim(col("ID_FUNCIONARIO")) != "")
    )
    .dropDuplicates(["ID_FUNCIONARIO", "ID_TIPO_DEFICIENCIA"])
    .withColumn("ingestion_timestamp", current_timestamp())
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Apply data-quality checks

# COMMAND ----------

if data_quality_config["reject_duplicate_business_keys"]:
    duplicate_count = (
        employee_disability_df
        .groupBy("ID_FUNCIONARIO", "ID_TIPO_DEFICIENCIA")
        .count()
        .filter(col("count") > 1)
        .limit(1)
        .count()
    )

    if duplicate_count > 0:
        raise ValueError(
            "Duplicate employee disability business keys remain after deduplication."
        )

# COMMAND ----------

# MAGIC %md
# MAGIC ## Write Delta source table

# COMMAND ----------

load_strategy = extraction_config["load_strategy"]

if load_strategy != "full_refresh":
    raise ValueError(
        f"Unsupported load strategy for employee disability: {load_strategy}"
    )

(
    employee_disability_df.write
    .format(storage_config["format"])
    .mode("overwrite")
    .option(
        "overwriteSchema",
        str(storage_config["allow_schema_overwrite"]).lower(),
    )
    .saveAsTable(target_table)
)

record_count = employee_disability_df.count()
print(
    "Employee disability ingestion completed successfully. "
    f"Records: {record_count}"
)
print(f"Target table: {target_table}")
