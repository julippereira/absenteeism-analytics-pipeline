# Databricks notebook source
# MAGIC %md
# MAGIC # Curated Delta Load
# MAGIC Persists the transformed employee, ICD, absenteeism, and occupational
# MAGIC accident datasets as curated Delta tables.
# MAGIC
# MAGIC This notebook expects the transformation notebooks to have created these
# MAGIC temporary views in the same Spark session:
# MAGIC
# MAGIC - `source_dim_employee`
# MAGIC - `source_dim_icd`
# MAGIC - `source_fact_absenteeism`
# MAGIC - `source_fact_occupational_accident`
# MAGIC
# MAGIC The public portfolio version uses an incremental Delta MERGE strategy.
# MAGIC Existing keys are updated, new keys are inserted, and records absent from the
# MAGIC current batch are preserved. Initial table creation is handled automatically.
# MAGIC Schemas, keys, partitions, and referential integrity are validated before load.

# COMMAND ----------

import json
from pathlib import Path
from typing import Dict, Iterable, List, Optional

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import DataType
from delta.tables import DeltaTable

# COMMAND ----------

# MAGIC %md
# MAGIC ## Load and validate configuration

# COMMAND ----------

CONFIG_PATH = "../../config/config.example.json"


def load_config(config_path: str) -> dict:
    """Load repository configuration and enforce secure public defaults."""
    path = Path(config_path)

    if not path.exists():
        raise FileNotFoundError(f"Configuration file not found: {config_path}")

    with path.open("r", encoding="utf-8") as config_file:
        config = json.load(config_file)

    security = config.get("security", {})
    privacy = config.get("privacy", {})
    storage = config.get("storage", {})

    if security.get("allow_plaintext_secrets", True):
        raise ValueError("Plaintext secrets must remain disabled.")

    if privacy.get("publish_real_data", True):
        raise ValueError("Public execution must not publish real personal data.")

    if storage.get("format", "").lower() != "delta":
        raise ValueError("The curated load layer requires Delta format.")

    return config


config = load_config(CONFIG_PATH)
storage_config = config["storage"]
curated_tables = config["tables"]["curated"]
data_quality_config = config["data_quality"]

catalog = storage_config["catalog"]
schema = storage_config["schema"]
write_format = storage_config["format"]
allow_schema_evolution = bool(storage_config["allow_schema_evolution"])
allow_schema_overwrite = bool(storage_config["allow_schema_overwrite"])
partition_config = storage_config.get("partition_columns", {})


def qualified_table(table_name: str) -> str:
    """Return a three-part catalog.schema.table identifier."""
    return f"{catalog}.{schema}.{table_name}"


# COMMAND ----------

# MAGIC %md
# MAGIC ## Declare source views and curated targets

# COMMAND ----------

LOAD_SPECIFICATIONS: Dict[str, dict] = {
    "employee_dimension": {
        "source_view": "source_dim_employee",
        "target_table": qualified_table(curated_tables["employee_dimension"]),
        "primary_key": "employee_key",
        "required_columns": [
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
        ],
        "partition_columns": [],
    },
    "icd_dimension": {
        "source_view": "source_dim_icd",
        "target_table": qualified_table(curated_tables["icd_dimension"]),
        "primary_key": "icd_key",
        "required_columns": [
            "icd_key",
            "icd_code",
            "icd_chapter",
            "icd_group",
            "icd_subgroup",
            "icd_category_code",
            "icd_category_description",
            "icd_subcategory_code",
            "icd_subcategory_description",
            "snapshot_timestamp",
        ],
        "partition_columns": [],
    },
    "absenteeism_fact": {
        "source_view": "source_fact_absenteeism",
        "target_table": qualified_table(curated_tables["absenteeism_fact"]),
        "primary_key": "absence_key",
        "required_columns": [
            "absence_key",
            "absence_id",
            "employee_key",
            "icd_key",
            "absence_start_date",
            "absence_end_date",
            "total_absence_days",
            "total_absence_hours",
            "absence_year",
            "snapshot_timestamp",
        ],
        "partition_columns": partition_config.get("fact_absenteeism", []),
    },
    "occupational_accident_fact": {
        "source_view": "source_fact_occupational_accident",
        "target_table": qualified_table(
            curated_tables["occupational_accident_fact"]
        ),
        "primary_key": "accident_key",
        "required_columns": [
            "accident_key",
            "accident_id",
            "employee_key",
            "accident_date",
            "absence_days",
            "accident_status",
            "accident_year",
            "snapshot_timestamp",
        ],
        "partition_columns": partition_config.get(
            "fact_occupational_accident",
            [],
        ),
    },
}

# COMMAND ----------

# MAGIC %md
# MAGIC ## Shared validation helpers

# COMMAND ----------


def require_view(view_name: str) -> None:
    """Fail when a required transformation view is unavailable."""
    if not spark.catalog.tableExists(view_name):
        raise RuntimeError(
            f"Required temporary view '{view_name}' was not found. "
            "Run all transformation notebooks in the same Spark session before "
            "executing the curated load."
        )


def require_columns(
    dataframe: DataFrame,
    required_columns: Iterable[str],
    dataset_name: str,
) -> None:
    """Fail when required curated columns are missing."""
    missing_columns = sorted(set(required_columns) - set(dataframe.columns))

    if missing_columns:
        raise ValueError(
            f"{dataset_name} is missing required columns: "
            + ", ".join(missing_columns)
        )


def validate_primary_key(
    dataframe: DataFrame,
    primary_key: str,
    dataset_name: str,
) -> None:
    """Validate that a curated primary key is non-null and unique."""
    null_key_exists = (
        dataframe
        .filter(F.col(primary_key).isNull())
        .limit(1)
        .count()
        > 0
    )

    if null_key_exists:
        raise ValueError(f"{dataset_name} contains a null {primary_key}.")

    duplicate_key_exists = (
        dataframe
        .groupBy(primary_key)
        .count()
        .filter(F.col("count") > 1)
        .limit(1)
        .count()
        > 0
    )

    if duplicate_key_exists:
        raise ValueError(f"{dataset_name} contains duplicate {primary_key} values.")


def validate_partition_columns(
    dataframe: DataFrame,
    partition_columns: Iterable[str],
    dataset_name: str,
) -> None:
    """Validate that configured partition columns exist and contain no nulls."""
    partition_columns = list(partition_columns)

    require_columns(dataframe, partition_columns, dataset_name)

    for partition_column in partition_columns:
        null_partition_exists = (
            dataframe
            .filter(F.col(partition_column).isNull())
            .limit(1)
            .count()
            > 0
        )

        if null_partition_exists:
            raise ValueError(
                f"{dataset_name} contains null values in partition column "
                f"'{partition_column}'."
            )


def validate_non_empty(dataframe: DataFrame, dataset_name: str) -> None:
    """Prevent a full refresh from replacing a target with an empty dataset."""
    if dataframe.limit(1).count() == 0:
        raise ValueError(
            f"{dataset_name} is empty. The existing curated table will not be "
            "overwritten."
        )


def schema_map(dataframe: DataFrame) -> Dict[str, DataType]:
    """Return a column-to-data-type map for schema comparison."""
    return {field.name: field.dataType for field in dataframe.schema.fields}


def validate_existing_target_schema(
    dataframe: DataFrame,
    target_table: str,
) -> None:
    """Block incompatible full refreshes when schema overwrite is disabled."""
    if not spark.catalog.tableExists(target_table):
        return

    if allow_schema_overwrite:
        return

    source_schema = schema_map(dataframe)
    target_schema = schema_map(spark.table(target_table))

    missing_in_source = sorted(set(target_schema) - set(source_schema))
    unexpected_in_source = sorted(set(source_schema) - set(target_schema))
    incompatible_types = sorted(
        column_name
        for column_name in set(source_schema).intersection(target_schema)
        if source_schema[column_name] != target_schema[column_name]
    )

    if missing_in_source or unexpected_in_source or incompatible_types:
        details = []

        if missing_in_source:
            details.append("missing in source: " + ", ".join(missing_in_source))

        if unexpected_in_source:
            details.append(
                "unexpected in source: " + ", ".join(unexpected_in_source)
            )

        if incompatible_types:
            details.append(
                "incompatible data types: " + ", ".join(incompatible_types)
            )

        raise ValueError(
            f"Schema mismatch for existing target '{target_table}' while "
            "allow_schema_overwrite is false. "
            + "; ".join(details)
        )


# COMMAND ----------

# MAGIC %md
# MAGIC ## Add controlled unknown dimension members

# COMMAND ----------


def typed_literal(value, data_type: DataType):
    """Create a literal cast to an existing DataFrame field type."""
    return F.lit(value).cast(data_type)


def add_unknown_employee_member(dataframe: DataFrame) -> DataFrame:
    """Add the employee member referenced by unmatched fact records."""
    unknown_employee_key = spark.range(1).select(
        F.sha2(F.lit("EMPLOYEE|UNKNOWN"), 256).alias("employee_key")
    ).first()["employee_key"]

    unknown_exists = (
        dataframe
        .filter(F.col("employee_key") == unknown_employee_key)
        .limit(1)
        .count()
        > 0
    )

    if unknown_exists:
        return dataframe

    values = {
        "employee_key": unknown_employee_key,
        "employee_hr_id": None,
        "employee_registration_id": None,
        "employee_name": "UNKNOWN EMPLOYEE",
        "job_title": "Unknown",
        "cost_center_id": None,
        "perimeter_name": "Unknown",
        "unit_name": "Unknown",
        "department_name": "Unknown",
        "gender": "NOT_INFORMED",
        "age": None,
        "age_group": "NOT_INFORMED",
        "disability_description": "NOT_INFORMED",
        "disability_classification": "NOT_INFORMED",
        "data_origin": "SYSTEM_MEMBER",
    }

    unknown_member = spark.range(1).select(
        *[
            (
                F.current_timestamp().cast(field.dataType).alias(field.name)
                if field.name == "snapshot_timestamp"
                else typed_literal(values.get(field.name), field.dataType).alias(
                    field.name
                )
            )
            for field in dataframe.schema.fields
        ]
    )

    return dataframe.unionByName(unknown_member)


# COMMAND ----------

# MAGIC %md
# MAGIC ## Read and prepare all curated datasets

# COMMAND ----------

for specification in LOAD_SPECIFICATIONS.values():
    require_view(specification["source_view"])

curated_dataframes: Dict[str, DataFrame] = {
    dataset_name: spark.table(specification["source_view"])
    for dataset_name, specification in LOAD_SPECIFICATIONS.items()
}

curated_dataframes["employee_dimension"] = add_unknown_employee_member(
    curated_dataframes["employee_dimension"]
)

# Cache because validation and writing trigger multiple actions.
for dataset_name, dataframe in curated_dataframes.items():
    curated_dataframes[dataset_name] = dataframe.cache()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Validate all datasets before writing any table

# COMMAND ----------

validation_results: Dict[str, int] = {}

for dataset_name, specification in LOAD_SPECIFICATIONS.items():
    dataframe = curated_dataframes[dataset_name]

    require_columns(
        dataframe,
        specification["required_columns"],
        dataset_name,
    )
    validate_non_empty(dataframe, dataset_name)
    validate_primary_key(
        dataframe,
        specification["primary_key"],
        dataset_name,
    )
    validate_partition_columns(
        dataframe,
        specification["partition_columns"],
        dataset_name,
    )
    validate_existing_target_schema(
        dataframe,
        specification["target_table"],
    )

    validation_results[dataset_name] = dataframe.count()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Validate referential integrity

# COMMAND ----------

employee_keys_df = (
    curated_dataframes["employee_dimension"]
    .select("employee_key")
    .dropDuplicates(["employee_key"])
)

icd_keys_df = (
    curated_dataframes["icd_dimension"]
    .select("icd_key")
    .dropDuplicates(["icd_key"])
)

orphan_absenteeism_employee_exists = (
    curated_dataframes["absenteeism_fact"]
    .select("employee_key")
    .dropDuplicates(["employee_key"])
    .join(employee_keys_df, on="employee_key", how="left_anti")
    .limit(1)
    .count()
    > 0
)

if orphan_absenteeism_employee_exists:
    raise ValueError(
        "The absenteeism fact contains employee keys not found in the employee "
        "dimension."
    )

orphan_accident_employee_exists = (
    curated_dataframes["occupational_accident_fact"]
    .select("employee_key")
    .dropDuplicates(["employee_key"])
    .join(employee_keys_df, on="employee_key", how="left_anti")
    .limit(1)
    .count()
    > 0
)

if orphan_accident_employee_exists:
    raise ValueError(
        "The occupational accident fact contains employee keys not found in the "
        "employee dimension."
    )

orphan_icd_exists = (
    curated_dataframes["absenteeism_fact"]
    .select("icd_key")
    .dropDuplicates(["icd_key"])
    .join(icd_keys_df, on="icd_key", how="left_anti")
    .limit(1)
    .count()
    > 0
)

if orphan_icd_exists:
    raise ValueError(
        "The absenteeism fact contains ICD keys not found in the ICD dimension."
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## Ensure the target catalog and schema exist

# COMMAND ----------

# Catalog creation normally requires elevated permissions and is intentionally not
# attempted here. The configured catalog must already exist.
spark.sql(f"CREATE SCHEMA IF NOT EXISTS `{catalog}`.`{schema}`")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Incrementally merge curated dimensions and facts

# COMMAND ----------

WRITE_ORDER: List[str] = [
    "employee_dimension",
    "icd_dimension",
    "absenteeism_fact",
    "occupational_accident_fact",
]


def is_delta_table(table_name: str) -> bool:
    """Return True when an existing catalog table uses the Delta provider."""
    if not spark.catalog.tableExists(table_name):
        return False

    detail = spark.sql(f"DESCRIBE DETAIL {table_name}").select("format").first()
    return bool(detail and str(detail["format"]).lower() == "delta")


def create_initial_delta_table(
    dataframe: DataFrame,
    target_table: str,
    partition_columns: Iterable[str],
) -> None:
    """Create a new curated Delta table from the first validated batch."""
    writer = (
        dataframe.write
        .format(write_format)
        .mode("errorifexists")
        .option("mergeSchema", str(allow_schema_evolution).lower())
    )

    partition_columns = list(partition_columns)
    if partition_columns:
        writer = writer.partitionBy(*partition_columns)

    writer.saveAsTable(target_table)


def merge_incremental_delta(
    dataframe: DataFrame,
    target_table: str,
    primary_key: str,
) -> None:
    """Update matching keys and insert new keys without deleting target history."""
    if not is_delta_table(target_table):
        raise ValueError(
            f"Existing target '{target_table}' is not a Delta table and cannot "
            "receive a MERGE operation."
        )

    if allow_schema_evolution:
        spark.conf.set("spark.databricks.delta.schema.autoMerge.enabled", "true")

    delta_target = DeltaTable.forName(spark, target_table)

    (
        delta_target.alias("target")
        .merge(
            dataframe.alias("source"),
            f"target.`{primary_key}` = source.`{primary_key}`",
        )
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute()
    )


def validate_source_keys_persisted(
    source_dataframe: DataFrame,
    target_table: str,
    primary_key: str,
    dataset_name: str,
) -> None:
    """Ensure every key from the current batch exists after the incremental load."""
    target_keys_df = (
        spark.table(target_table)
        .select(primary_key)
        .dropDuplicates([primary_key])
    )

    missing_source_key_exists = (
        source_dataframe
        .select(primary_key)
        .dropDuplicates([primary_key])
        .join(target_keys_df, on=primary_key, how="left_anti")
        .limit(1)
        .count()
        > 0
    )

    if missing_source_key_exists:
        raise RuntimeError(
            f"Incremental load validation failed for {dataset_name}: at least "
            "one source key was not persisted."
        )


load_results: Dict[str, dict] = {}

for dataset_name in WRITE_ORDER:
    specification = LOAD_SPECIFICATIONS[dataset_name]
    dataframe = curated_dataframes[dataset_name]
    target_table = specification["target_table"]
    primary_key = specification["primary_key"]
    partition_columns = specification["partition_columns"]
    target_existed = spark.catalog.tableExists(target_table)

    if target_existed:
        merge_incremental_delta(
            dataframe=dataframe,
            target_table=target_table,
            primary_key=primary_key,
        )
        load_action = "MERGE"
    else:
        create_initial_delta_table(
            dataframe=dataframe,
            target_table=target_table,
            partition_columns=partition_columns,
        )
        load_action = "CREATE"

    validate_source_keys_persisted(
        source_dataframe=dataframe,
        target_table=target_table,
        primary_key=primary_key,
        dataset_name=dataset_name,
    )

    target_count = spark.table(target_table).count()

    load_results[dataset_name] = {
        "target_table": target_table,
        "source_record_count": validation_results[dataset_name],
        "target_record_count": target_count,
        "partition_columns": partition_columns,
        "action": load_action,
    }

# COMMAND ----------

# MAGIC ## Apply post-load quality checks

# COMMAND ----------

for dataset_name in WRITE_ORDER:
    specification = LOAD_SPECIFICATIONS[dataset_name]
    persisted_df = spark.table(specification["target_table"])

    validate_non_empty(persisted_df, dataset_name)
    validate_primary_key(
        persisted_df,
        specification["primary_key"],
        dataset_name,
    )
    validate_partition_columns(
        persisted_df,
        specification["partition_columns"],
        dataset_name,
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## Release cached datasets and report results

# COMMAND ----------

for dataframe in curated_dataframes.values():
    dataframe.unpersist()

print("Curated Delta incremental load completed successfully.")

for dataset_name in WRITE_ORDER:
    result = load_results[dataset_name]
    partition_text = (
        ", ".join(result["partition_columns"])
        if result["partition_columns"]
        else "none"
    )

    print(
        f"- {dataset_name}: action={result['action']} | "
        f"source_records={result['source_record_count']} | "
        f"target_records={result['target_record_count']} | "
        f"table={result['target_table']} | partitions={partition_text}"
    )
