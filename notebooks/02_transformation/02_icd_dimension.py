# Databricks notebook source
# MAGIC %md
# MAGIC # ICD Dimension Transformation
# MAGIC Builds the curated ICD-10 analytical dimension by consolidating diagnosis
# MAGIC codes from current absenteeism, detailed medical leave, and historical
# MAGIC absenteeism sources.
# MAGIC
# MAGIC This notebook performs transformation only. It creates the temporary view
# MAGIC `source_dim_icd`; physical Delta persistence is handled by the load layer.
# MAGIC
# MAGIC The hierarchy implemented here is an analytical classification layer for the
# MAGIC portfolio solution. It does not replace an official licensed terminology
# MAGIC service or a complete clinical coding reference.

# COMMAND ----------

import json
import re
from pathlib import Path
from typing import Dict, Iterable

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, StructField, StructType
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
):
    """Select the first available source column from an alias list."""
    for candidate in candidates:
        if candidate in dataframe.columns:
            return F.col(candidate).cast("string").alias(output_name)

    if required and data_quality_config["fail_on_schema_mismatch"]:
        raise ValueError(
            f"No source column was found for '{output_name}'. "
            f"Expected one of: {', '.join(candidates)}"
        )

    return F.lit(None).cast("string").alias(output_name)


def null_if_blank(column_name: str):
    """Convert blanks and textual null markers to actual null values."""
    value = F.trim(F.col(column_name).cast("string"))
    return F.when(
        value.isNull()
        | (value == "")
        | F.upper(value).isin("NULL", "NONE", "N/A", "NA", "VAZIO", "0"),
        F.lit(None),
    ).otherwise(value)


def standardize_icd_code(column_name: str):
    """Return the first valid ICD code in normalized category/subcategory form."""
    first_code = F.upper(
        F.trim(F.element_at(F.split(F.col(column_name).cast("string"), ","), 1))
    )
    alphanumeric_code = F.regexp_replace(first_code, "[^A-Z0-9]", "")

    return (
        F.when(
            first_code.isNull()
            | (first_code == "")
            | first_code.isin("NULL", "NONE", "N/A", "NA", "VAZIO", "0"),
            F.lit(None),
        )
        .when(
            F.length(alphanumeric_code) >= 4,
            F.concat(
                F.substring(alphanumeric_code, 1, 3),
                F.lit("."),
                F.substring(alphanumeric_code, 4, 4),
            ),
        )
        .when(
            F.length(alphanumeric_code) == 3,
            alphanumeric_code,
        )
        .otherwise(F.lit(None))
    )


# COMMAND ----------

# MAGIC %md
# MAGIC ## Read source tables

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

if data_quality_config["fail_on_empty_required_source"]:
    source_record_found = any(
        dataframe.limit(1).count() > 0
        for dataframe in [
            absenteeism_raw_df,
            medical_leave_details_raw_df,
            historical_absenteeism_raw_df,
        ]
    )

    if not source_record_found:
        raise ValueError("All ICD source datasets are empty.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Extract diagnosis codes from each source

# COMMAND ----------

absenteeism_icd_df = (
    absenteeism_raw_df
    .select(
        first_existing_column(
            absenteeism_raw_df,
            ["cid_principal", "codigo_cid", "id_cid", "icd_code"],
            "raw_icd_code",
        ),
        first_existing_column(
            absenteeism_raw_df,
            ["descricao_cid", "desc_doenca", "diagnosis_description"],
            "source_description",
            required=False,
        ),
    )
    .withColumn("source_system", F.lit("CURRENT_ABSENTEEISM"))
)

medical_leave_icd_df = (
    medical_leave_details_raw_df
    .select(
        first_existing_column(
            medical_leave_details_raw_df,
            ["cids", "cid", "codigo_cid", "icd_code"],
            "raw_icd_code",
        ),
        first_existing_column(
            medical_leave_details_raw_df,
            ["descricaomotivo", "descricao_motivo", "diagnosis_description"],
            "source_description",
            required=False,
        ),
    )
    .withColumn("source_system", F.lit("MEDICAL_LEAVE"))
)

historical_icd_df = (
    historical_absenteeism_raw_df
    .select(
        first_existing_column(
            historical_absenteeism_raw_df,
            ["cid", "codigo_cid", "id_cid", "icd_code"],
            "raw_icd_code",
        ),
        first_existing_column(
            historical_absenteeism_raw_df,
            ["doenca", "descricao_cid", "diagnosis_description"],
            "source_description",
            required=False,
        ),
    )
    .withColumn("source_system", F.lit("HISTORICAL_ABSENTEEISM"))
)

icd_source_df = (
    absenteeism_icd_df
    .unionByName(medical_leave_icd_df, allowMissingColumns=True)
    .unionByName(historical_icd_df, allowMissingColumns=True)
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Normalize ICD codes and source descriptions

# COMMAND ----------

icd_normalized_df = (
    icd_source_df
    .withColumn("icd_code", standardize_icd_code("raw_icd_code"))
    .withColumn(
        "source_description",
        null_if_blank("source_description"),
    )
    .filter(F.col("icd_code").isNotNull())
    .withColumn(
        "icd_category_code",
        F.substring(F.regexp_replace(F.col("icd_code"), "[^A-Z0-9]", ""), 1, 3),
    )
    .withColumn("icd_subcategory_code", F.col("icd_code"))
    .withColumn("icd_letter", F.substring(F.col("icd_category_code"), 1, 1))
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Build analytical ICD-10 hierarchy

# COMMAND ----------

icd_hierarchy_df = (
    icd_normalized_df
    .withColumn(
        "icd_chapter",
        F.when(
            F.col("icd_letter").isin("A", "B"),
            F.lit("[A00-B99] Certain infectious and parasitic diseases"),
        )
        .when(
            F.col("icd_letter") == "C",
            F.lit("[C00-D48] Neoplasms"),
        )
        .when(
            F.col("icd_letter") == "D",
            F.when(
                F.col("icd_category_code") <= "D48",
                F.lit("[C00-D48] Neoplasms"),
            ).otherwise(
                F.lit("[D50-D89] Diseases of the blood and immune mechanism")
            ),
        )
        .when(
            F.col("icd_letter") == "E",
            F.lit("[E00-E90] Endocrine, nutritional and metabolic diseases"),
        )
        .when(
            F.col("icd_letter") == "F",
            F.lit("[F00-F99] Mental and behavioural disorders"),
        )
        .when(
            F.col("icd_letter") == "G",
            F.lit("[G00-G99] Diseases of the nervous system"),
        )
        .when(
            F.col("icd_letter") == "H",
            F.when(
                F.col("icd_category_code") <= "H59",
                F.lit("[H00-H59] Diseases of the eye and adnexa"),
            ).otherwise(
                F.lit("[H60-H95] Diseases of the ear and mastoid process")
            ),
        )
        .when(
            F.col("icd_letter") == "I",
            F.lit("[I00-I99] Diseases of the circulatory system"),
        )
        .when(
            F.col("icd_letter") == "J",
            F.lit("[J00-J99] Diseases of the respiratory system"),
        )
        .when(
            F.col("icd_letter") == "K",
            F.lit("[K00-K93] Diseases of the digestive system"),
        )
        .when(
            F.col("icd_letter") == "L",
            F.lit("[L00-L99] Diseases of the skin and subcutaneous tissue"),
        )
        .when(
            F.col("icd_letter") == "M",
            F.lit("[M00-M99] Diseases of the musculoskeletal system"),
        )
        .when(
            F.col("icd_letter") == "N",
            F.lit("[N00-N99] Diseases of the genitourinary system"),
        )
        .when(
            F.col("icd_letter") == "O",
            F.lit("[O00-O99] Pregnancy, childbirth and the puerperium"),
        )
        .when(
            F.col("icd_letter") == "P",
            F.lit("[P00-P96] Conditions originating in the perinatal period"),
        )
        .when(
            F.col("icd_letter") == "Q",
            F.lit("[Q00-Q99] Congenital malformations and chromosomal abnormalities"),
        )
        .when(
            F.col("icd_letter") == "R",
            F.lit("[R00-R99] Symptoms, signs and abnormal findings"),
        )
        .when(
            F.col("icd_letter").isin("S", "T"),
            F.lit("[S00-T98] Injury, poisoning and other external consequences"),
        )
        .when(
            F.col("icd_letter").isin("V", "W", "X", "Y"),
            F.lit("[V01-Y98] External causes of morbidity and mortality"),
        )
        .when(
            F.col("icd_letter") == "Z",
            F.lit("[Z00-Z99] Factors influencing health status and service contact"),
        )
        .when(
            F.col("icd_letter") == "U",
            F.lit("[U00-U99] Codes for special purposes"),
        )
        .otherwise(F.lit("[OTHER] Other conditions")),
    )
    .withColumn(
        "icd_group",
        F.when(F.col("icd_letter").isin("A", "B"), F.lit("Infectious and parasitic diseases"))
        .when(F.col("icd_letter").isin("C", "D"), F.lit("Neoplasms, blood and immune diseases"))
        .when(F.col("icd_letter") == "E", F.lit("Endocrine, nutritional and metabolic diseases"))
        .when(F.col("icd_letter") == "F", F.lit("Mental and behavioural disorders"))
        .when(F.col("icd_letter") == "G", F.lit("Nervous system diseases"))
        .when(F.col("icd_letter") == "H", F.lit("Eye and ear diseases"))
        .when(F.col("icd_letter") == "I", F.lit("Circulatory system diseases"))
        .when(F.col("icd_letter") == "J", F.lit("Respiratory system diseases"))
        .when(F.col("icd_letter") == "K", F.lit("Digestive system diseases"))
        .when(F.col("icd_letter") == "L", F.lit("Skin and subcutaneous tissue diseases"))
        .when(F.col("icd_letter") == "M", F.lit("Musculoskeletal system diseases"))
        .when(F.col("icd_letter") == "N", F.lit("Genitourinary system diseases"))
        .when(F.col("icd_letter") == "O", F.lit("Pregnancy, childbirth and puerperium"))
        .when(F.col("icd_letter") == "P", F.lit("Perinatal conditions"))
        .when(F.col("icd_letter") == "Q", F.lit("Congenital malformations"))
        .when(F.col("icd_letter") == "R", F.lit("Symptoms and abnormal findings"))
        .when(F.col("icd_letter").isin("S", "T"), F.lit("Injuries and trauma"))
        .when(F.col("icd_letter").isin("V", "W", "X", "Y"), F.lit("External causes"))
        .when(F.col("icd_letter") == "Z", F.lit("Health service contact factors"))
        .when(F.col("icd_letter") == "U", F.lit("Special-purpose codes"))
        .otherwise(F.lit("Other groups")),
    )
    .withColumn(
        "icd_subgroup",
        F.when(F.col("icd_category_code").between("J00", "J06"), F.lit("[J00-J06] Acute upper respiratory infections"))
        .when(F.col("icd_category_code").between("J09", "J18"), F.lit("[J09-J18] Influenza and pneumonia"))
        .when(F.col("icd_category_code").between("J20", "J22"), F.lit("[J20-J22] Other acute lower respiratory infections"))
        .when(F.col("icd_category_code").between("J40", "J47"), F.lit("[J40-J47] Chronic lower respiratory diseases"))
        .when(F.col("icd_category_code").between("M00", "M25"), F.lit("[M00-M25] Arthropathies"))
        .when(F.col("icd_category_code").between("M40", "M54"), F.lit("[M40-M54] Dorsopathies"))
        .when(F.col("icd_category_code").between("M60", "M79"), F.lit("[M60-M79] Soft tissue disorders"))
        .when(F.col("icd_category_code").between("F10", "F19"), F.lit("[F10-F19] Disorders due to psychoactive substances"))
        .when(F.col("icd_category_code").between("F30", "F39"), F.lit("[F30-F39] Mood disorders"))
        .when(F.col("icd_category_code").between("F40", "F48"), F.lit("[F40-F48] Neurotic and stress-related disorders"))
        .when(F.col("icd_category_code").between("K20", "K31"), F.lit("[K20-K31] Diseases of oesophagus, stomach and duodenum"))
        .when(F.col("icd_category_code").between("I10", "I15"), F.lit("[I10-I15] Hypertensive diseases"))
        .when(F.col("icd_category_code").between("Z00", "Z99"), F.lit("[Z00-Z99] Health service contact factors"))
        .otherwise(
            F.concat(
                F.lit("["),
                F.col("icd_category_code"),
                F.lit("] Specific analytical subgroup"),
            )
        ),
    )
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Apply curated category and subcategory names

# COMMAND ----------

category_name = (
    F.when(F.col("icd_category_code") == "M54", F.lit("Dorsalgia"))
    .when(F.col("icd_category_code") == "M50", F.lit("Cervical disc disorders"))
    .when(F.col("icd_category_code") == "M51", F.lit("Other intervertebral disc disorders"))
    .when(F.col("icd_category_code") == "M75", F.lit("Shoulder lesions"))
    .when(F.col("icd_category_code") == "J06", F.lit("Acute upper respiratory infections"))
    .when(F.col("icd_category_code") == "J18", F.lit("Pneumonia, organism unspecified"))
    .when(F.col("icd_category_code") == "F41", F.lit("Other anxiety disorders"))
    .when(F.col("icd_category_code") == "F32", F.lit("Depressive episodes"))
    .when(F.col("icd_category_code") == "K29", F.lit("Gastritis and duodenitis"))
    .when(F.col("icd_category_code") == "Z34", F.lit("Supervision of normal pregnancy"))
    .when(F.col("icd_category_code") == "Z29", F.lit("Need for other prophylactic measures"))
    .otherwise(F.coalesce(F.col("source_description"), F.lit("General category")))
)

subcategory_name = (
    F.when(F.col("icd_subcategory_code") == "M54.5", F.lit("Low back pain"))
    .when(F.col("icd_subcategory_code") == "M54.4", F.lit("Lumbago with sciatica"))
    .when(F.col("icd_subcategory_code") == "M54.2", F.lit("Cervicalgia"))
    .when(F.col("icd_subcategory_code") == "M54.3", F.lit("Sciatica"))
    .when(F.col("icd_subcategory_code") == "J06.9", F.lit("Acute upper respiratory infection, unspecified"))
    .when(F.col("icd_subcategory_code") == "J18.9", F.lit("Pneumonia, unspecified"))
    .when(F.col("icd_subcategory_code") == "F41.1", F.lit("Generalized anxiety disorder"))
    .when(F.col("icd_subcategory_code") == "F41.2", F.lit("Mixed anxiety and depressive disorder"))
    .when(F.col("icd_subcategory_code") == "F32.9", F.lit("Depressive episode, unspecified"))
    .when(F.col("icd_subcategory_code") == "Z34.9", F.lit("Supervision of normal pregnancy, unspecified"))
    .when(F.col("icd_subcategory_code") == "Z29.0", F.lit("Isolation"))
    .when(F.col("icd_subcategory_code") == "U07.1", F.lit("COVID-19, virus identified"))
    .otherwise(F.coalesce(F.col("source_description"), F.lit("General subcategory")))
)

icd_named_df = (
    icd_hierarchy_df
    .withColumn("icd_category_name", category_name)
    .withColumn("icd_subcategory_name", subcategory_name)
    .withColumn(
        "icd_category_description",
        F.concat_ws(
            " - ",
            F.col("icd_category_code"),
            F.col("icd_category_name"),
        ),
    )
    .withColumn(
        "icd_subcategory_description",
        F.concat_ws(
            " - ",
            F.col("icd_subcategory_code"),
            F.col("icd_subcategory_name"),
        ),
    )
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Select the most complete description per ICD code

# COMMAND ----------

source_priority = (
    F.when(F.col("source_system") == "CURRENT_ABSENTEEISM", F.lit(3))
    .when(F.col("source_system") == "MEDICAL_LEAVE", F.lit(2))
    .otherwise(F.lit(1))
)

description_completeness = F.when(
    F.col("source_description").isNotNull(),
    F.length(F.col("source_description")),
).otherwise(F.lit(0))

icd_dedup_window = Window.partitionBy("icd_code").orderBy(
    description_completeness.desc(),
    source_priority.desc(),
    F.col("icd_subcategory_description").asc(),
)

icd_dimension_base_df = (
    icd_named_df
    .withColumn("row_number", F.row_number().over(icd_dedup_window))
    .filter(F.col("row_number") == 1)
    .drop("row_number")
    .select(
        "icd_code",
        "icd_chapter",
        "icd_group",
        "icd_subgroup",
        "icd_category_code",
        "icd_category_description",
        "icd_subcategory_code",
        "icd_subcategory_description",
    )
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Add unknown and not-reported members

# COMMAND ----------

special_member_schema = StructType(
    [
        StructField("icd_code", StringType(), False),
        StructField("icd_chapter", StringType(), False),
        StructField("icd_group", StringType(), False),
        StructField("icd_subgroup", StringType(), False),
        StructField("icd_category_code", StringType(), False),
        StructField("icd_category_description", StringType(), False),
        StructField("icd_subcategory_code", StringType(), False),
        StructField("icd_subcategory_description", StringType(), False),
    ]
)

special_members = [
    (
        "NO_ICD",
        "[NO_ICD] Chapter not reported",
        "[NO_ICD] Group not reported",
        "[NO_ICD] Subgroup not reported",
        "NO_ICD",
        "NO_ICD - Category not reported",
        "NO_ICD",
        "NO_ICD - Medical record without an ICD code",
    ),
    (
        "UNKNOWN",
        "[UNKNOWN] Unknown chapter",
        "[UNKNOWN] Unknown group",
        "[UNKNOWN] Unknown subgroup",
        "UNKNOWN",
        "UNKNOWN - Unrecognized category",
        "UNKNOWN",
        "UNKNOWN - Unrecognized or invalid ICD code",
    ),
]

special_members_df = spark.createDataFrame(
    special_members,
    schema=special_member_schema,
)

icd_dimension_df = (
    icd_dimension_base_df
    .unionByName(special_members_df)
    .dropDuplicates(["icd_code"])
    .withColumn(
        "icd_key",
        F.sha2(
            F.concat_ws("|", F.lit("ICD"), F.col("icd_code")),
            256,
        ),
    )
    .withColumn("snapshot_timestamp", F.current_timestamp())
    .select(
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
    )
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Apply final data-quality checks

# COMMAND ----------

if icd_dimension_df.limit(1).count() == 0:
    raise ValueError("The ICD dimension transformation produced no records.")

null_key_count = (
    icd_dimension_df
    .filter(F.col("icd_key").isNull() | F.col("icd_code").isNull())
    .limit(1)
    .count()
)

if null_key_count > 0:
    raise ValueError("The ICD dimension contains null analytical keys.")

if data_quality_config["reject_duplicate_business_keys"]:
    duplicate_code_count = (
        icd_dimension_df
        .groupBy("icd_code")
        .count()
        .filter(F.col("count") > 1)
        .limit(1)
        .count()
    )

    if duplicate_code_count > 0:
        raise ValueError("The ICD dimension contains duplicate ICD codes.")

required_special_members = {"NO_ICD", "UNKNOWN"}
available_special_members = {
    row["icd_code"]
    for row in (
        icd_dimension_df
        .filter(F.col("icd_code").isin(*required_special_members))
        .select("icd_code")
        .collect()
    )
}

if available_special_members != required_special_members:
    raise ValueError("Required special ICD dimension members are missing.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Register output view

# COMMAND ----------

OUTPUT_VIEW = "source_dim_icd"
icd_dimension_df.createOrReplaceTempView(OUTPUT_VIEW)

record_count = icd_dimension_df.count()
print(
    "ICD dimension transformation completed successfully. "
    f"Records: {record_count}"
)
print(f"Temporary view created: {OUTPUT_VIEW}")
