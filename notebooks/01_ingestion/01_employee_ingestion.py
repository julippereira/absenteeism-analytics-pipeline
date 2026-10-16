# Databricks notebook source
# MAGIC %md
# MAGIC # Employee Master Ingestion
# MAGIC Extracts employee master data from the configured API, applies minimum
# MAGIC source-layer transformations, enriches records with organizational
# MAGIC perimeter data, and writes the result to a Delta source table.
# MAGIC
# MAGIC This public portfolio version contains no credentials, internal endpoints,
# MAGIC company identifiers, or real employee data.

# COMMAND ----------

import json
from pathlib import Path

import requests
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, current_timestamp, floor, months_between, to_date

# COMMAND ----------

# MAGIC %md
# MAGIC ## Load public configuration

# COMMAND ----------

CONFIG_PATH = "../../config/config.example.json"


def load_config(config_path: str) -> dict:
    """Load the repository configuration and enforce secure defaults."""
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
extraction_config = config["extractions"]["employee_master"]
security_config = config["security"]
api_config = config["api"]
storage_config = config["storage"]

if not extraction_config.get("enabled", False):
    raise RuntimeError("Employee master extraction is disabled in the configuration.")

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
# MAGIC ## Build target table names

# COMMAND ----------

catalog = storage_config["catalog"]
schema = storage_config["schema"]
target_table = f"{catalog}.{schema}.{extraction_config['target_table']}"
perimeter_table = (
    f"{catalog}.{schema}."
    f"{config['tables']['sources']['organizational_perimeter']}"
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Request employee master data

# COMMAND ----------

request_parameters = {
    "empresa": company_id,
    "codigo": report_code,
    "chave": api_key,
    "tipoSaida": api_config["output_format"],
    "ativo": "Sim",
    "inativo": "Sim",
    "afastado": "Sim",
    "pendente": "Sim",
    "ferias": "Sim",
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
    if config["data_quality"]["fail_on_empty_required_source"]:
        raise ValueError("The employee master API returned no records.")
    dbutils.notebook.exit("No employee records returned. Target table unchanged.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create and validate source DataFrame

# COMMAND ----------

employee_raw_df = spark.createDataFrame(response_data)

required_columns = {
    "DATA_NASCIMENTO",
    "CCUSTO",
}
missing_columns = sorted(required_columns - set(employee_raw_df.columns))

if missing_columns and config["data_quality"]["fail_on_schema_mismatch"]:
    raise ValueError(
        "Required source columns are missing: " + ", ".join(missing_columns)
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## Apply source-layer transformations

# COMMAND ----------

employee_transformed_df: DataFrame = (
    employee_raw_df
    .withColumn(
        "birth_date_parsed",
        to_date(col("DATA_NASCIMENTO"), "dd/MM/yyyy"),
    )
    .withColumn(
        "IDADE",
        floor(
            months_between(current_timestamp(), col("birth_date_parsed")) / 12
        ).cast("int"),
    )
    .drop("birth_date_parsed")
    .withColumn("ingestion_timestamp", current_timestamp())
)

# Auxiliary organizational mapping. No real mapping data is stored in the repo.
organizational_perimeter_df = spark.table(perimeter_table)

employee_enriched_df = employee_transformed_df.join(
    organizational_perimeter_df,
    on="CCUSTO",
    how="left",
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Write Delta source table

# COMMAND ----------

load_strategy = extraction_config["load_strategy"]

if load_strategy != "full_refresh":
    raise ValueError(
        f"Unsupported load strategy for employee master: {load_strategy}"
    )

(
    employee_enriched_df.write
    .format(storage_config["format"])
    .mode("overwrite")
    .option(
        "overwriteSchema",
        str(storage_config["allow_schema_overwrite"]).lower(),
    )
    .saveAsTable(target_table)
)

record_count = employee_enriched_df.count()
print(f"Employee master ingestion completed successfully. Records: {record_count}")
print(f"Target table: {target_table}")
