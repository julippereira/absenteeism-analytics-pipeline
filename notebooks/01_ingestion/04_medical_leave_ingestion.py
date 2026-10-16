# Databricks notebook source
# MAGIC %md
# MAGIC # Medical Leave Ingestion
# MAGIC Extracts historical medical leave records from the configured API in
# MAGIC API-compliant date windows, applies minimum source-layer typing and
# MAGIC validation, and writes a full-refresh Delta source table.
# MAGIC
# MAGIC This public portfolio version contains no credentials, internal endpoints,
# MAGIC company identifiers, or real employee and health data.

# COMMAND ----------

import json
import time
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Iterator, Tuple

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, current_timestamp, to_date, trim, when

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
    api = config.get("api", {})

    if security.get("allow_plaintext_secrets", True):
        raise ValueError("Plaintext secrets must remain disabled.")

    if not security.get("tls_verification", False):
        raise ValueError("TLS verification must remain enabled.")

    if not api.get("verify_ssl", False):
        raise ValueError("API SSL verification must remain enabled.")

    return config


config = load_config(CONFIG_PATH)
extraction_config = config["extractions"]["medical_leave"]
security_config = config["security"]
api_config = config["api"]
storage_config = config["storage"]
data_quality_config = config["data_quality"]

if not extraction_config.get("enabled", False):
    raise RuntimeError("Medical leave extraction is disabled in the configuration.")

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
# MAGIC ## Resolve extraction period

# COMMAND ----------


def parse_iso_date(value: str, field_name: str) -> date:
    """Parse an ISO date and reject example placeholders."""
    if not value or value == "YYYY-MM-DD":
        raise ValueError(
            f"{field_name} must be configured as a valid YYYY-MM-DD date."
        )

    try:
        return datetime.strptime(value, "%Y-%m-%d").date()
    except ValueError as exc:
        raise ValueError(
            f"Invalid {field_name}: expected YYYY-MM-DD."
        ) from exc


def resolve_end_date(rule: str) -> date:
    """Resolve a dynamic or fixed extraction end date."""
    normalized_rule = rule.strip().lower()

    if normalized_rule == "yesterday":
        return date.today() - timedelta(days=1)

    if normalized_rule == "today":
        return date.today()

    return parse_iso_date(rule, "end_date_rule")


start_date = parse_iso_date(
    extraction_config["historical_start_date"],
    "historical_start_date",
)
end_date = resolve_end_date(extraction_config["end_date_rule"])

if start_date > end_date:
    raise ValueError("The historical start date cannot be after the end date.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Generate API-compliant date windows

# COMMAND ----------


def generate_date_intervals(
    start: date,
    end: date,
    window_days: int,
) -> Iterator[Tuple[date, date]]:
    """Yield inclusive intervals containing no more than window_days dates."""
    if window_days <= 0:
        raise ValueError("window_days must be greater than zero.")

    current_start = start

    while current_start <= end:
        current_end = min(
            current_start + timedelta(days=window_days - 1),
            end,
        )
        yield current_start, current_end
        current_start = current_end + timedelta(days=1)


date_intervals = list(
    generate_date_intervals(
        start=start_date,
        end=end_date,
        window_days=int(api_config["max_window_days"]),
    )
)

print(f"Prepared {len(date_intervals)} secure extraction window(s).")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create retry-enabled HTTP session

# COMMAND ----------


def create_http_session() -> requests.Session:
    """Create a session with bounded retries for transient API failures."""
    retry_policy = Retry(
        total=int(api_config["max_retries"]),
        connect=int(api_config["max_retries"]),
        read=int(api_config["max_retries"]),
        status=int(api_config["max_retries"]),
        backoff_factor=float(api_config["retry_backoff_seconds"]),
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=frozenset(["GET"]),
        raise_on_status=False,
    )

    adapter = HTTPAdapter(max_retries=retry_policy)
    session = requests.Session()
    session.mount("https://", adapter)
    return session


http_session = create_http_session()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Extract historical medical leave records

# COMMAND ----------

records = []

for interval_number, (interval_start, interval_end) in enumerate(
    date_intervals,
    start=1,
):
    request_parameters = {
        "empresa": company_id,
        "codigo": report_code,
        "chave": api_key,
        "tipoSaida": api_config["output_format"],
        "listaFuncionarios": "",
        "dataInicio": interval_start.strftime("%d/%m/%Y"),
        "dataFim": interval_end.strftime("%d/%m/%Y"),
    }

    # Credentials, parameters, URLs, and response bodies are never logged.
    response = http_session.get(
        api_url,
        params={"parametro": json.dumps(request_parameters)},
        timeout=int(api_config["timeout_seconds"]),
        verify=bool(api_config["verify_ssl"]),
    )
    response.raise_for_status()

    try:
        batch = response.json()
    except requests.exceptions.JSONDecodeError as exc:
        raise ValueError(
            f"The API returned invalid JSON in extraction window {interval_number}."
        ) from exc

    if isinstance(batch, dict):
        batch = [batch]

    if not isinstance(batch, list):
        raise TypeError(
            f"Unexpected API response type in extraction window {interval_number}."
        )

    records.extend(batch)
    print(
        f"Extraction window {interval_number}/{len(date_intervals)} completed. "
        f"Records received: {len(batch)}"
    )

    if interval_number < len(date_intervals):
        time.sleep(float(api_config["request_interval_seconds"]))

http_session.close()

if not records:
    if data_quality_config["fail_on_empty_required_source"]:
        raise ValueError("The medical leave API returned no records.")

    dbutils.notebook.exit(
        "No medical leave records returned. Target table unchanged."
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create and validate source DataFrame

# COMMAND ----------

medical_leave_raw_df = spark.createDataFrame(records)

required_columns = {
    "CODIGOSEQUENCIALLICENCA",
    "DATAINICIOAFASTAMENTO",
    "DATAFIMAFASTAMENTO",
}

missing_columns = sorted(required_columns - set(medical_leave_raw_df.columns))

if missing_columns and data_quality_config["fail_on_schema_mismatch"]:
    raise ValueError(
        "Required source columns are missing: " + ", ".join(missing_columns)
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## Apply source-layer transformations

# COMMAND ----------

DATE_COLUMNS = [
    "DATAFICHA",
    "DATAFIMATESTADO",
    "DATAINICIOATESTADO",
    "DATASOLICITACAO",
    "DATAINICIOAFASTAMENTO",
    "DATAFIMAFASTAMENTO",
]

BOOLEAN_COLUMNS = [
    "ABONADO",
    "ACIDENTETRAJETO",
    "IMPRIMIRCID",
    "IMPRIMIRMOTIVO",
    "IMPRIMIRSOLICITANTE",
    "EXISTELICENCAMESMOMOTIVO60DIAS",
    "ATESTADOENCAMINHADOPREVIDENCIA",
    "AFASTAMENTOENCAMINHADOPREVIDENCIA",
]

medical_leave_df: DataFrame = medical_leave_raw_df

for column_name in DATE_COLUMNS:
    if column_name in medical_leave_df.columns:
        medical_leave_df = medical_leave_df.withColumn(
            column_name,
            when(
                col(column_name).isNull()
                | (trim(col(column_name).cast("string")) == ""),
                None,
            ).otherwise(to_date(col(column_name), "dd/MM/yyyy")),
        )

for column_name in BOOLEAN_COLUMNS:
    if column_name in medical_leave_df.columns:
        medical_leave_df = medical_leave_df.withColumn(
            column_name,
            when(trim(col(column_name).cast("string")) == "1", True)
            .when(trim(col(column_name).cast("string")) == "0", False)
            .otherwise(None)
            .cast("boolean"),
        )

# Business identifiers remain strings to preserve leading zeros and avoid overflow.
medical_leave_df = (
    medical_leave_df
    .withColumn(
        "CODIGOSEQUENCIALLICENCA",
        col("CODIGOSEQUENCIALLICENCA").cast("string"),
    )
    .withColumn("ingestion_timestamp", current_timestamp())
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Apply data-quality checks

# COMMAND ----------

invalid_key_count = (
    medical_leave_df
    .filter(
        col("CODIGOSEQUENCIALLICENCA").isNull()
        | (trim(col("CODIGOSEQUENCIALLICENCA")) == "")
    )
    .limit(1)
    .count()
)

if invalid_key_count > 0:
    raise ValueError("Medical leave records with an empty business key were found.")

if data_quality_config["reject_duplicate_business_keys"]:
    duplicate_key_count = (
        medical_leave_df
        .groupBy("CODIGOSEQUENCIALLICENCA")
        .count()
        .filter(col("count") > 1)
        .limit(1)
        .count()
    )

    if duplicate_key_count > 0:
        raise ValueError(
            "Duplicate medical leave business keys were found in the API response."
        )

# COMMAND ----------

# MAGIC %md
# MAGIC ## Write full-refresh Delta source table

# COMMAND ----------

load_strategy = extraction_config["load_strategy"]

if load_strategy != "full_refresh":
    raise ValueError(
        f"Unsupported load strategy for medical leave: {load_strategy}"
    )

catalog = storage_config["catalog"]
schema = storage_config["schema"]
target_table = f"{catalog}.{schema}.{extraction_config['target_table']}"

(
    medical_leave_df.write
    .format(storage_config["format"])
    .mode("overwrite")
    .option(
        "mergeSchema",
        str(storage_config["allow_schema_evolution"]).lower(),
    )
    .option(
        "overwriteSchema",
        str(storage_config["allow_schema_overwrite"]).lower(),
    )
    .saveAsTable(target_table)
)

record_count = medical_leave_df.count()
print(
    "Medical leave ingestion completed successfully. "
    f"Records: {record_count}"
)
print(f"Target table: {target_table}")
