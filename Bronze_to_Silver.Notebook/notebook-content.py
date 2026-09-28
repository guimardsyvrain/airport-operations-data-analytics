# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "f226cea8-9e70-4963-b9be-ef9139876743",
# META       "default_lakehouse_name": "LH_Bronze",
# META       "default_lakehouse_workspace_id": "a7eb2f42-6705-4658-9ff5-c11724fe1696",
# META       "known_lakehouses": [
# META         {
# META           "id": "e597456d-02ee-4a2a-82c9-a90c65d73a24"
# META         },
# META         {
# META           "id": "f226cea8-9e70-4963-b9be-ef9139876743"
# META         }
# META       ]
# META     }
# META   }
# META }

# MARKDOWN ********************

# # Airport Analytics - Data cleaning, transformation and cleaning steps.
# 
# This notebook transforms raw airport data from the Bronze Lakehouse into clean,
# validated and standardized datasets for the Silver Lakehouse.
# 
# The notebook processes data from four source groups:
# 
# 1. **Azure SQL relational database** – airlines, gates and flights.
# 2. **GitHub file-based datasets** – passenger flow, commercial transactions,
#    operational targets and sustainability data.
# 3. **REST API** – flight operational events and baggage events in JSON format.
# 4. **Passenger feedback** – unstructured text data.
# 
# For each dataset, the notebook will:
# 
# - Load the raw Bronze data.
# - Profile the dataset and identify data-quality issues.
# - Clean and standardize the data.
# - Apply validation and business rules.
# - Verify the cleaned dataset.
# - Store the validated data in the Silver Lakehouse.
# 
# The Silver datasets will subsequently support dimensional modelling, KPI calculations and Power BI reporting.

# MARKDOWN ********************

# ## 0.1 Notebook Setup
# 
# The following libraries and PySpark functions support data profiling,
# cleaning, validation and transformation throughout the notebook.

# CELL ********************

# Core PySpark modules
from pyspark.sql import functions as F
from pyspark.sql import types as T
from pyspark.sql.window import Window

# Common functions used for cleaning and validation
from pyspark.sql.functions import (
    col,
    when,
    lit,
    trim,
    lower,
    upper,
    count,
    countDistinct,
    sum,
    avg,
    min,
    max,
    isnan,
    isnull,
    to_date,
    to_timestamp,
    regexp_replace,
    row_number
)

# Python utilities
from datetime import datetime

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # 1. Azure SQL – Relational Data
# 
# This section processes the relational airport data originally extracted from
# Azure SQL and stored in the Bronze Lakehouse.
# 
# The datasets include:
# 
# - Airlines
# - Gates
# - Flights
# 
# Each dataset will first be profiled to identify missing values, duplicates,
# invalid records, inconsistent formats and relationship issues before any
# cleaning is performed.

# MARKDOWN ********************

# ## 1.1 Airlines
# 
# The Airlines dataset provides reference information about airlines operating
# at the airport.
# 
# The objective is to verify the dataset's structure, uniqueness of airline
# codes, completeness and consistency before storing the validated data in the
# Silver Lakehouse.

# CELL ********************

# Load Airlines dataset from Bronze
df_airlines = spark.read.table("Airlines")

display(df_airlines)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Data Quality Assessment
# 
# Before applying transformations, the Airlines dataset is profiled to identify
# potential data-quality issues including duplicate airline codes, missing values,
# inconsistent categorical values and formatting problems.

# CELL ********************

# Basic dataset information

print(f"Total rows: {df_airlines.count()}")
print(f"Total columns: {len(df_airlines.columns)}")

df_airlines.printSchema()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Check missing values

df_airlines.select([
    F.count(F.when(F.col(c).isNull(), c)).alias(c)
    for c in df_airlines.columns
]).show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Check duplicate airline codes
df_airlines.groupBy("airline_code") \
    .count() \
    .filter(F.col("count") > 1) \
    .show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Inspect airline codes and service types
df_airlines.select("airline_code").distinct().show()

df_airlines.select("service_type").distinct().show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### **Airlines** : Data quality findings
# The Airlines dataset contains 6 records and 3 columns.
# 
# The quality assessment identified:
# - No missing values.
# - No duplicate airline codes.
# - Airline codes are unique and consistently formatted.
# - Service type values are complete and represent valid service categories.
# 
# No corrective transformation is required. The dataset can therefore be
# stored in the Silver Lakehouse without modifying its business values.

# MARKDOWN ********************

# ### Save Airlines to Silver Lakehouse

# CELL ********************

# Write validated Airlines dataset to Silver Lakehouse
silver_path = "abfss://Airport_operations_analytics@onelake.dfs.fabric.microsoft.com/LH_Silver.Lakehouse/Tables"

df_airlines.write.format("delta").mode("overwrite").save(f"{silver_path}/Tables/Airlines")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 1.2 Gates
# 
# The Gates dataset provides reference information about airport gates, including
# their terminal zones and aircraft capacity classifications.
# 
# The dataset is assessed for completeness, duplicate gate identifiers,
# inconsistent categorical values and formatting issues before being validated
# and stored in the Silver Lakehouse.

# MARKDOWN ********************

# ##### 1. Load Gates from Bronze

# CELL ********************

# Load Gates dataset from Bronze
df_gates = spark.read.table("Gates")

display(df_gates)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ##### 2. Basic profiling

# CELL ********************

print(f"Total rows: {df_gates.count()}")
print(f"Total columns: {len(df_gates.columns)}")

df_gates.printSchema()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ##### 3. Check missing values

# CELL ********************

# Check missing values
df_gates.select([
    F.count(F.when(F.col(c).isNull(), c)).alias(c)
    for c in df_gates.columns
]).show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ##### 4. Check duplicate gate_id

# CELL ********************

df_gates.groupBy("gate_id") \
    .count() \
    .filter(F.col("count") > 1) \
    .show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ##### 5. Inspect categorical values

# CELL ********************

print("Terminal zones:")
df_gates.select("terminal_zone").distinct().show(truncate=False)

print("Capacity classes:")
df_gates.select("capacity_class").distinct().show(truncate=False)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### **Gates** : Data Quality Findings
# 
# The Gates dataset contains 15 records and 3 attributes.
# 
# The quality assessment identified:
# - No missing values.
# - No duplicate gate identifiers.
# - Terminal zones are complete and consistently categorized as Domestic, Transborder and International.
# - Capacity classes are consistently categorized as Narrow-body and Wide-body.
# 
# No corrective transformation is required. The dataset can be stored in the
# Silver Lakehouse without modifying its business values.

# MARKDOWN ********************

# ### Save Gates datasets to Silver Lakehouse

# CELL ********************

# Write validated Gates dataset to Silver Lakehouse
silver_path = "abfss://Airport_operations_analytics@onelake.dfs.fabric.microsoft.com/LH_Silver.Lakehouse/Tables"

df_gates.write.format("delta").mode("overwrite").save(f"{silver_path}/Tables/Gates")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 1.3 Flights
# 
# The Flights dataset contains flight-level operational information including
# airlines, routes, scheduled and actual times, gate assignments, passenger
# volumes, flight status, delays and gate occupancy times.
# 
# The dataset is profiled to identify missing values, duplicate flight identifiers,
# invalid reference values, inconsistent categories, timestamp issues and unusual
# operational values before cleaning and validation.

# MARKDOWN ********************

# ##### 1.3.1 Load and inspect

# CELL ********************

# Load Flights from Bronze

df_flights = spark.read.table("Flights")

print(f"Total rows: {df_flights.count()}")
print(f"Total columns: {len(df_flights.columns)}")

df_flights.printSchema()
display(df_flights.limit(10))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ##### 1.3.2. Check missing values

# CELL ********************

# Missing values by column

df_flights.select([
    F.count(
        F.when(F.col(c).isNull(), c)
    ).alias(c)
    for c in df_flights.columns
]).show(vertical=True)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Investigate records with operational nulls

df_flights.filter(
    col("actual_time").isNull() |
    col("delay_minutes").isNull() |
    col("gate_in_time").isNull() |
    col("gate_out_time").isNull()
).select(
    "flight_id",
    "status",
    "scheduled_time",
    "actual_time",
    "delay_minutes",
    "gate_in_time",
    "gate_out_time"
).show(30, truncate=False)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ##### I cross check the missing values with the flight status. I cane to understand that every flight with delay has actual time, delay minutes gate in time and gate out time. So it's rationale that we have missing value for these fo0ur columns: actual_time, delay_minute, gate_in_tim, gate_out_time.

# CELL ********************

df_flights.filter(
    col("passenger_count").isNull()
).show(truncate=False)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ##### 1.3.3 Check duplicate flight IDs

# CELL ********************

df_flights.groupBy("flight_id") \
    .count() \
    .filter(F.col("count") > 1) \
    .show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ##### 1.3.4. Inspect important categorical fields

# CELL ********************

for c in [
    "airline_code",
    "direction",
    "traffic_type",
    "status",
    "delay_reason"
]:
    print(f"\n--- {c} ---")
    df_flights.select(c).distinct().show(truncate=False)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ##### 1.3.5. Check operational ranges

# CELL ********************

df_flights.select(
    F.min("delay_minutes").alias("min_delay"),
    F.max("delay_minutes").alias("max_delay"),
    F.avg("delay_minutes").alias("avg_delay"),
    F.min("passenger_count").alias("min_passengers"),
    F.max("passenger_count").alias("max_passengers"),
    F.min("seat_capacity").alias("min_capacity"),
    F.max("seat_capacity").alias("max_capacity")
).show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ##### Inspect the top 10 rows with maximum delay ordered by descending

# CELL ********************

df_flights.orderBy(
    col("delay_minutes").desc()
).select(
    "flight_id",
    "status",
    "scheduled_time",
    "actual_time",
    "delay_minutes",
    "delay_reason"
).show(10, truncate=False)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ##### 1.3.6 Check business/reference integrity

# CELL ********************

# Flights referencing an unknown airline

df_flights.join(
    df_airlines.select("airline_code"),
    on="airline_code",
    how="left_anti"
).select("flight_id", "airline_code").show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Flights referencing an unknown gate

df_flights.join(
    df_gates.select("gate_id"),
    on="gate_id",
    how="left_anti"
).select("flight_id", "gate_id").show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Passenger count should not exceed aircraft seat capacity

df_flights.filter(
    F.col("passenger_count") > F.col("seat_capacity")
).select(
    "flight_id",
    "passenger_count",
    "seat_capacity"
).show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### **Flights** : Data quality findings
# 
# The Flights dataset contains 700 flight records.
# 
# The assessment identified the following issues:
# 
# - One invalid airline code (`Air Canada`) instead of the standardized code `AC`.
# - An inconsistent traffic type (`DOM`) instead of `Domestic`.
# - One invalid gate assignment (`Gate-99`) that does not exist in the Gates reference table.
# - One missing passenger count for flight F0028.
# - 18 cancelled flights have no actual time, delay minutes or gate occupancy timestamps. These are valid business nulls and are retained.
# - Several flights have significant delays above two hours. These values are internally consistent with the scheduled and actual timestamps and are retained as valid operational events.
# - No duplicate flight identifiers were identified.
# - No flights have passenger counts exceeding aircraft seat capacity.

# MARKDOWN ********************

# ##### Clean the identified issues

# CELL ********************

# Clean and standardize Flights

df_flights_clean = (
    df_flights

    # Standardize airline code
    .withColumn(
        "airline_code",
        F.when(F.col("airline_code") == "Air Canada", "AC")
         .otherwise(F.col("airline_code"))
    )

    # Standardize traffic type
    .withColumn(
        "traffic_type",
        F.when(F.col("traffic_type") == "DOM", "Domestic")
         .otherwise(F.col("traffic_type"))
    )

    # Remove invalid gate assignment
    .withColumn(
        "gate_id",
        F.when(F.col("gate_id") == "Gate-99", F.lit(None))
         .otherwise(F.col("gate_id"))
    )
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Save the Flights dataset to Silver

# CELL ********************

# Write validated Gates dataset to Silver Lakehouse
silver_path = "abfss://Airport_operations_analytics@onelake.dfs.fabric.microsoft.com/LH_Silver.Lakehouse/Tables"

df_flights_clean.write.format("delta").mode("overwrite").save(f"{silver_path}/Tables/Flights")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # 2. File-Based Data
# 
# This section processes structured CSV datasets originally stored in GitHub and
# ingested into the Bronze Lakehouse through Microsoft Fabric pipelines.
# 
# The datasets include:
# 
# - Passenger Flow
# - Commercial Transactions
# - Operational Targets
# - Sustainability Daily
# 
# Each dataset is profiled to identify missing values, duplicates, inconsistent
# categories, invalid values and business-rule violations. Validated datasets are
# then stored as Delta tables in the Silver Lakehouse.

# MARKDOWN ********************

# ##### 2.1 Passenger flow
# 
# The Passenger Flow dataset contains hourly passenger volumes and security
# wait-time information by traffic type and terminal zone.
# 
# The dataset is assessed for completeness, duplicates, category consistency,
# invalid passenger volumes and unusual security wait times before being
# validated for the Silver layer.

# MARKDOWN ********************

# ##### 2.1.1 Load Passenger Flow from Bronze

# CELL ********************

df_passenger = spark.read.table("passenger_flow_raw")

print(f"Total rows: {df_passenger.count()}")
print(f"Total columns: {len(df_passenger.columns)}")

df_passenger.printSchema()
display(df_passenger.limit(10))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ##### 2.1.2 Check nulls value

# CELL ********************

df_passenger.select([
    F.count(F.when(F.col(c).isNull(), c)).alias(c)
    for c in df_passenger.columns
]).show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ##### 2.1.3 Check duplicates

# CELL ********************

df_passenger.groupBy(
    "date", "hour", "traffic_type"
).count().filter(F.col("count") > 1).show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ##### 2.1.4 Inspect categorical columns

# CELL ********************

df_passenger.select("traffic_type").distinct().show()
df_passenger.select("terminal_zone").distinct().show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ##### 2.1.5 check range value

# CELL ********************

df_passenger.select(
    F.min("passengers_processed").alias("min_passengers"),
    F.max("passengers_processed").alias("max_passengers"),
    F.min("avg_security_wait_min").alias("min_wait"),
    F.max("avg_security_wait_min").alias("max_wait")
).show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ##### 2.1.6 Clean the dataset

# CELL ********************

# Clean Passenger Flow

df_passenger_clean = (
    df_passenger
    .withColumn("date", F.to_date("date"))
    .withColumn("hour", F.col("hour").cast("int"))
    .withColumn(
        "traffic_type",
        F.when(F.col("traffic_type") == "INTL", "International")
         .otherwise(F.col("traffic_type"))
    )
    .withColumn(
        "terminal_zone",
        F.when(
            F.col("terminal_zone").isNull() &
            (F.col("traffic_type") == "International"),
            "International"
        ).otherwise(F.col("terminal_zone"))
    )
    .withColumn(
        "passengers_processed",
        F.col("passengers_processed").cast("int")
    )
    .withColumn(
        "avg_security_wait_min",
        F.col("avg_security_wait_min").cast("double")
    )
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ##### 2.1.7 Rerun check range value

# CELL ********************

df_passenger_clean.select(
    F.min("passengers_processed").alias("min_passengers"),
    F.max("passengers_processed").alias("max_passengers"),
    F.min("avg_security_wait_min").alias("min_wait"),
    F.max("avg_security_wait_min").alias("max_wait")
).show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ##### 2.1.8 Reinspect categorical columns

# CELL ********************

df_passenger_clean.printSchema()

df_passenger_clean.select("traffic_type").distinct().show()
df_passenger_clean.select("terminal_zone").distinct().show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Passenger Flow: Data quality findings
# 
# The Passenger Flow dataset was assessed and standardized for the Silver layer.
# 
# The following issues were identified and addressed:
# 
# - Standardized `INTL` to `International`.
# - Resolved the associated missing terminal zone.
# - Converted date, hour, passenger volume and security wait-time fields to appropriate data types.
# - One missing passenger count was retained as NULL to avoid introducing an unsupported value.
# - No duplicate records were identified.
# - Passenger volumes and security wait times were validated after type conversion.

# MARKDOWN ********************

# ### Save Passenger Flow to Silver

# CELL ********************

# Write cleaned Passenger Flow dataset to Silver
silver_path = "abfss://Airport_operations_analytics@onelake.dfs.fabric.microsoft.com/LH_Silver.Lakehouse/Tables"

df_passenger_clean.write.format("delta").mode("overwrite").save(f"{silver_path}/Tables/passenger_flow")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 2.2 Commercial Transactions
# 
# The Commercial Transactions dataset contains transaction-level information
# related to airport commercial activities.
# 
# The dataset is assessed for missing values, duplicate transactions,
# inconsistent categories, invalid amounts and data-type issues before being
# cleaned and validated for the Silver layer.

# MARKDOWN ********************

# ##### 2.2.1 Load and inspect the dataset

# CELL ********************

df_commercial = spark.read.table("commercial_transactions_raw")

print(f"Total rows: {df_commercial.count()}")
print(f"Total columns: {len(df_commercial.columns)}")

df_commercial.printSchema()
display(df_commercial.limit(10))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ##### 2.2.2 Check missing values

# CELL ********************

df_commercial.select([
    F.count(F.when(F.col(c).isNull(), c)).alias(c)
    for c in df_commercial.columns
]).show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ##### 2.2.3 Check duplicates in column key

# CELL ********************

df_commercial.groupBy("transaction_id") \
    .count() \
    .filter(F.col("count") > 1) \
    .show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ##### 2.2.4 Inspect categorical values

# CELL ********************

df_commercial.select("revenue_category").distinct().show(truncate=False)

df_commercial.select("payment_method").distinct().show(truncate=False)

df_commercial.select("terminal_zone").distinct().show(truncate=False)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ##### 2.2.5 Clean the dataset

# CELL ********************

df_commercial_clean = (
    df_commercial
    .withColumn(
        "timestamp",
        F.to_timestamp("timestamp")
    )
    .withColumn(
        "amount_cad",
        F.col("amount_cad").cast("double")
    )
    .withColumn(
        "revenue_category",
        F.when(
            F.col("revenue_category") == "F&B",
            "Food & Beverage"
        ).otherwise(F.col("revenue_category"))
    )
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### **Commercial Transactions** : Data quality findings
# 
# The Commercial Transactions dataset contains 500 records.
# 
# The quality assessment and cleaning identified:
# 
# - No duplicate transaction identifiers.
# - All timestamps were successfully converted to timestamp format.
# - `amount_cad` was converted from string to numeric format.
# - `F&B` was standardized to `Food & Beverage`.
# - One missing transaction amount was retained as NULL to avoid introducing an unsupported value.
# - 14 negative transactions were retained because they represent plausible refunds or adjustments.
# - Payment methods and terminal zones were complete and consistently categorized.

# MARKDOWN ********************

# ### Save the 'Commercial Transactions' dataset to Silver Lakehouse

# CELL ********************

# Write Commercial Transactions to Silver
silver_path = "abfss://Airport_operations_analytics@onelake.dfs.fabric.microsoft.com/LH_Silver.Lakehouse/Tables"

df_commercial_clean.write.format("delta").mode("overwrite").save(f"{silver_path}/Tables/commercial_transactions")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 2.3 Operational Targets
# 
# The Operational Targets dataset defines performance thresholds used to evaluate
# airport KPIs across operational, passenger experience, commercial and
# sustainability areas.
# 
# The dataset is assessed for completeness, duplicate KPI definitions,
# appropriate data types, valid target directions and consistency of business
# rules before being stored in the Silver Lakehouse.

# MARKDOWN ********************

# ##### 2.3.1 Load and inspect the dataset

# CELL ********************

df_targets = spark.read.table("operational_targets_raw")

print(f"Total rows: {df_targets.count()}")
print(f"Total columns: {len(df_targets.columns)}")

df_targets.printSchema()
display(df_targets)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ##### 2.3.2 Check missing values

# CELL ********************

df_targets.select([
    F.count(F.when(F.col(c).isNull(), c)).alias(c)
    for c in df_targets.columns
]).show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ##### 2.3.3 Check duplicate KPI definitions

# CELL ********************

df_targets.groupBy("kpi_name") \
    .count() \
    .filter(F.col("count") > 1) \
    .show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ##### Clean the dataset

# CELL ********************

# Correct Operational Targets data types

df_targets_clean = (
    df_targets
    .withColumn(
        "target_value",
        F.col("target_value").cast("double")
    )
    .withColumn(
        "effective_start_date",
        F.to_date("effective_start_date")
    )
    .withColumn(
        "effective_end_date",
        F.to_date("effective_end_date")
    )
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Data Quality Findings
# 
# The Operational Targets dataset contains 7 KPI target definitions.
# 
# The quality assessment and validation identified:
# 
# - No missing KPI names, business areas, units, target values, target directions or effective start dates.
# - No duplicate KPI definitions.
# - target_value was converted from string to numeric format.
# - effective_start_date and effective_end_date were converted to date format.
# - All target directions are consistently defined using `>=` or `<=`.
# - NULL effective end dates were retained because they represent currently active targets.

# MARKDOWN ********************

# ### Save the dataset to Silver Lakehouse

# CELL ********************

# Write Operational Targets to Silver
silver_path = "abfss://Airport_operations_analytics@onelake.dfs.fabric.microsoft.com/LH_Silver.Lakehouse/Tables"

df_targets_clean.write.format("delta").mode("overwrite").save(f"{silver_path}/Tables/operational_targets")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 2.4 Sustainability Daily
# 
# The Sustainability daily dataset contains daily environmental performance
# measures used to monitor airport sustainability objectives.
# 
# The dataset is assessed for completeness, duplicate dates, appropriate data
# types, invalid values and unusual observations before being cleaned.

# MARKDOWN ********************

# ##### 2.4.1 Load and inspect

# CELL ********************

df_sustainability = spark.read.table("sustainability_daily_raw")

print(f"Total rows: {df_sustainability.count()}")
print(f"Total columns: {len(df_sustainability.columns)}")

df_sustainability.printSchema()
display(df_sustainability.limit(10))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ##### 2.4.2 Check missing values

# CELL ********************

df_sustainability.select([
    F.count(F.when(F.col(c).isNull(), c)).alias(c)
    for c in df_sustainability.columns
]).show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ##### 2.4.3 Check duplicate dates

# CELL ********************

df_sustainability.groupBy("date") \
    .count() \
    .filter(F.col("count") > 1) \
    .show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ##### 2.4.4 Convert the data types

# CELL ********************

df_sustainability_clean = (
    df_sustainability
    .withColumn("date", F.to_date("date"))
    .withColumn("electricity_kwh", F.col("electricity_kwh").cast("double"))
    .withColumn("water_m3", F.col("water_m3").cast("double"))
    .withColumn("waste_tonnes", F.col("waste_tonnes").cast("double"))
    .withColumn(
        "estimated_co2e_tonnes",
        F.col("estimated_co2e_tonnes").cast("double")
    )
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ##### 2.5.5 Inspect the data range of the numeric columns

# CELL ********************

df_sustainability_clean.printSchema()

df_sustainability_clean.select(
    F.min("electricity_kwh").alias("min_electricity"),
    F.max("electricity_kwh").alias("max_electricity"),
    F.min("water_m3").alias("min_water"),
    F.max("water_m3").alias("max_water"),
    F.min("waste_tonnes").alias("min_waste"),
    F.max("waste_tonnes").alias("max_waste"),
    F.min("estimated_co2e_tonnes").alias("min_co2e"),
    F.max("estimated_co2e_tonnes").alias("max_co2e")
).show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ##### 2.5.6 Inspect the missing water value

# CELL ********************

df_sustainability_clean.filter(
    F.col("water_m3").isNull()
).show(truncate=False)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ##### Handle the missing value observed for water_m3
# 
# **Rationale** : The missing water consumption value was imputed using the average of the previous and following day (428.85 m³). This local interpolation preserves the short-term pattern of the daily time series without relying on a broader monthly average.

# CELL ********************

from pyspark.sql.window import Window

# Define windows based on date
w_prev = Window.orderBy("date").rowsBetween(
    Window.unboundedPreceding, -1
)

w_next = Window.orderBy("date").rowsBetween(
    1, Window.unboundedFollowing
)

# Find previous and next non-null water values
df_sustainability_clean = (
    df_sustainability_clean
    .withColumn(
        "prev_water",
        F.last("water_m3", ignorenulls=True).over(w_prev)
    )
    .withColumn(
        "next_water",
        F.first("water_m3", ignorenulls=True).over(w_next)
    )
    .withColumn(
        "water_m3",
        F.when(
            F.col("water_m3").isNull(),
            (F.col("prev_water") + F.col("next_water")) / 2
        ).otherwise(F.col("water_m3"))
    )
    .drop("prev_water", "next_water")
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Verify if water_m3 missing value has been well imput.
df_sustainability_clean.filter(
    F.col("date") == "2026-08-09"
).show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Data Quality Findings
# 
# The Sustainability Daily dataset contains 31 daily records.
# 
# The quality assessment and cleaning identified:
# 
# - No duplicate dates.
# - Date and sustainability measures were converted to appropriate date and numeric data types.
# - One missing `water_m3` value was identified.
# - The missing value was dynamically imputed using the nearest valid observations before and after the missing record.
# - No negative values were identified across electricity, water, waste, or estimated CO₂e measures.
# - Numeric ranges were reviewed and retained as valid observations.

# MARKDOWN ********************

# ### Save the dataset into Silver Lakehouse

# CELL ********************

# Write Sustainability Daily to Silver
silver_path = "abfss://Airport_operations_analytics@onelake.dfs.fabric.microsoft.com/LH_Silver.Lakehouse/Tables"

df_sustainability_clean.write.format("delta").mode("overwrite").save(f"{silver_path}/Tables/sustainability_daily")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # 3. REST API – JSON Data
# 
# This section processes operational data retrieved from REST API endpoints in
# JSON format and ingested into the Bronze Lakehouse.
# 
# The datasets include:
# 
# - Flight Operational Events
# - Baggage Events
# 
# The datasets are assessed for schema consistency, missing values, duplicates,
# invalid identifiers, timestamp issues and operational inconsistencies before
# being transformed and stored in the Silver Lakehouse.

# MARKDOWN ********************

# ##### 3.1 Flight operational events
# 
# The Flight operational events dataset contains event-level information generated
# during airport flight operations.
# 
# The dataset is profiled to validate its structure, flight references, event
# types, timestamps and completeness before transformation into the Silver layer.

# MARKDOWN ********************

# ##### 3.1.1 Load and inspect the dataset

# CELL ********************

# Load flight operational events from Bronze

df_flight_events_raw = (
    spark.read
    .json("Files/api/flight_operational_events.json")
)

# print the number of records from df_flight_events_raw
print("Total records:", df_flight_events_raw.count())

# print the dataset schema 
df_flight_events_raw.printSchema()

# display the top 10 rows of the dataset
display(df_flight_events_raw.limit(10))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ##### 3.1.2 Flatten the flight operational events dataset

# CELL ********************

df_flight_events = df_flight_events_raw.select(
    "event_id",

    F.col("flight.flight_id").alias("flight_id"),
    F.col("flight.flight_number").alias("flight_number"),
    F.col("flight.airline").alias("airline_code"),

    F.col("operation.direction").alias("direction"),
    F.col("operation.gate").alias("gate_id"),
    F.col("operation.status").alias("status"),

    F.to_timestamp("timing.scheduled").alias("scheduled_time"),
    F.to_timestamp("timing.actual").alias("actual_time"),
    F.col("timing.delay.minutes").alias("delay_minutes"),
    F.col("timing.delay.reason").alias("delay_reason"),

    "source_system"
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ##### 3.1.3 Inspect df_flight_events structure

# CELL ********************

# print the row and column number of the dataset
print(f"Total rows: {df_flight_events.count()}")
print(f"Total columns: {len(df_flight_events.columns)}")

# print the schema of the dataset
df_flight_events.printSchema()

# display the top 10 rows of the dataset
display(df_flight_events.limit(10))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ##### 3.1.4 Checking missing values

# CELL ********************

df_flight_events.select([
    F.count(F.when(F.col(c).isNull(), c)).alias(c)
    for c in df_flight_events.columns
]).show(vertical=True)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Check if the missing actual_time and delay_minutes are resulted from cancel flights
df_flight_events.filter(
    F.col("actual_time").isNull() |
    F.col("delay_minutes").isNull()
).select(
    "event_id",
    "flight_id",
    "status",
    "gate_id",
    "actual_time",
    "delay_minutes"
).show(truncate=False)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ##### 3.1.5 Check duplicate event_id

# CELL ********************

df_flight_events.groupBy("event_id") \
    .count() \
    .filter(F.col("count") > 1) \
    .show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ##### 3.1.6 Inspect categorical values

# CELL ********************

for c in ["airline_code", "direction", "status", "source_system"]:
    print(f"\n- {c} -")
    df_flight_events.select(c).distinct().show(truncate=False)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ##### 3.1.7 Check delay ranges

# CELL ********************

df_flight_events.select(
    F.min("delay_minutes").alias("min_delay"),
    F.max("delay_minutes").alias("max_delay"),
    F.avg("delay_minutes").alias("avg_delay")
).show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ##### 3.1.8 Validate references against Silver master data

# CELL ********************

# Check flight_id
invalid_flights = df_flight_events.join(
    df_flights_clean.select("flight_id"),
    on="flight_id",
    how="left_anti"
)

invalid_flights.select("event_id", "flight_id").show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Check Airlines
invalid_airlines = df_flight_events.join(
    df_airlines.select("airline_code"),
    on="airline_code",
    how="left_anti"
)

invalid_airlines.select(
    "event_id", "airline_code"
).show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Check gates, excluding legitimate nulls
invalid_gates = (
    df_flight_events
    .filter(F.col("gate_id").isNotNull())
    .join(
        df_gates.select("gate_id"),
        on="gate_id",
        how="left_anti"
    )
)

invalid_gates.select(
    "event_id", "flight_id", "gate_id"
).show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ##### 3.1.8 Clean the identified issues

# CELL ********************

df_flight_events_clean = (
    df_flight_events

    # Standardize airline code
    .withColumn(
        "airline_code",
        F.when(F.col("airline_code") == "Air Canada", "AC")
         .otherwise(F.col("airline_code"))
    )

    # Invalid gate reference
    .withColumn(
        "gate_id",
        F.when(F.col("gate_id") == "Gate-99", F.lit(None))
         .otherwise(F.col("gate_id"))
    )
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### **Flights operational events** : Data quality findings
# 
# The Flight Operational Events dataset contains 250 event records.
# 
# The quality assessment and cleaning identified:
# 
# - No duplicate event identifiers.
# - All flight identifiers successfully matched the Flights reference dataset.
# - Air Canada was standardized to the airline code AC.
# - One invalid gate reference (Gate-99) was converted to NULL.
# - All remaining non-null gate and airline references passed referential-integrity validation.
# - Seven records have NULL actual times and delay minutes. These records correspond to cancelled flights and were retained as valid business nulls.
# - Flight direction, status and source-system categories were consistent.
# - Delay values ranged from 0 to 97 minutes and were retained as valid operational observations.

# MARKDOWN ********************

# ### Save to Silver Lakehouse

# CELL ********************

# Write Flight Operational Events to Silver
silver_path = "abfss://Airport_operations_analytics@onelake.dfs.fabric.microsoft.com/LH_Silver.Lakehouse/Tables"

df_flight_events_clean.write.format("delta").mode("overwrite").save(f"{silver_path}/Tables/flight_operational_events")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 3.2 Baggage Events
# 
# The Baggage Events dataset contains flight-level baggage handling information,
# including baggage volumes, mishandled bags, and first- and last-bag delivery times.
# 
# The nested JSON structure is flattened and assessed for completeness, duplicate
# event identifiers, flight-reference integrity, and invalid operational values
# before being stored in the Silver Lakehouse.

# MARKDOWN ********************

# ##### 3.2.1 Read the baggage_events JSON file

# CELL ********************

# Load the dataset
df_baggage_raw = (spark.read.json("Files/api/baggage_events"))

# Check the row count
print(f"Total top-level records: {df_baggage_raw.count()}")

# Display and print the schema
df_baggage_raw.printSchema()
display(df_baggage_raw)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ##### 3.2.2 Flatten the baggage_events nested JSON file

# CELL ********************

# Flatten the files
df_baggage = df_baggage_raw.select(
    "bag_event_id",
    "flight_id",
    "bags_processed",
    "mishandled_bags",
    F.col("arrival.first_bag_min").alias("first_bag_min"),
    F.col("arrival.last_bag_min").alias("last_bag_min")
)

# print the number of rows and columns
print(f"Total rows: {df_baggage.count()}")
print(f"Total columns: {len(df_baggage.columns)}")

# Display the dataset schema and top 10 first row
df_baggage.printSchema()
display(df_baggage.limit(10))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ##### 3.2.3 Check missing values

# CELL ********************

df_baggage.select([
    F.count(F.when(F.col(c).isNull(), c)).alias(c)
    for c in df_baggage.columns
]).show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ##### 3.2.4 Check duplicate event IDs

# CELL ********************

df_baggage.groupBy("bag_event_id") \
    .count() \
    .filter(F.col("count") > 1) \
    .show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ##### 3.2.5 Check numeric ranges

# CELL ********************

df_baggage.select(
    F.min("bags_processed").alias("min_bags"),
    F.max("bags_processed").alias("max_bags"),
    F.min("mishandled_bags").alias("min_mishandled"),
    F.max("mishandled_bags").alias("max_mishandled"),
    F.min("first_bag_min").alias("min_first_bag"),
    F.max("first_bag_min").alias("max_first_bag"),
    F.min("last_bag_min").alias("min_last_bag"),
    F.max("last_bag_min").alias("max_last_bag")
).show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ##### 3.2.6 Check business rules 

# CELL ********************

# Mishandled bags cannot exceed total bags processed
df_baggage.filter(
    F.col("mishandled_bags") > F.col("bags_processed")
).show()

# Last bag should not be delivered before the first bag
df_baggage.filter(
    F.col("last_bag_min") < F.col("first_bag_min")
).show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ##### 3.2.7 Validate flight references

# CELL ********************

# Join the datasets df_baggage and df_flights_clean to cross-check the flight_id 
invalid_baggage_flights = df_baggage.join(
    df_flights_clean.select("flight_id"),
    on="flight_id",
    how="left_anti"
)

invalid_baggage_flights.select("bag_event_id", "flight_id").show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### **Baggage_events** : Data quality findings
# 
# The Baggage Events dataset contains 300 baggage event records.
# 
# The quality assessment identified:
# 
# - No missing values.
# - No duplicate baggage event identifiers.
# - All flight identifiers successfully matched the Flights reference dataset.
# - No cases where mishandled bags exceeded total bags processed.
# - No cases where last-bag delivery time occurred before first-bag delivery time.
# - The nested JSON structure was successfully flattened into an analytics-ready tabular structure.
# 
# No corrective transformation is required. The flattened dataset is ready for the Silver Lakehouse.

# MARKDOWN ********************

# ### Save to Silver Lakehouse

# CELL ********************

# Write Flight Operational Events to Silver
silver_path = "abfss://Airport_operations_analytics@onelake.dfs.fabric.microsoft.com/LH_Silver.Lakehouse/Tables"

df_baggage.write.format("delta").mode("overwrite").save(f"{silver_path}/Tables/baggage_events")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # 4. Passenger feedback – Text Data
# 
# This section processes passenger feedback data containing structured attributes
# and free-text comments.
# 
# The dataset is assessed for completeness, duplicate records, data-type consistency,
# categorical consistency and text quality. The objective is to prepare reliable
# passenger feedback data for downstream analysis and reporting.

# MARKDOWN ********************

# ##### 4.1 Load and inspect the dataset

# CELL ********************

# Read the passenger_feedback file
df_feedback = (
    spark.read
    .option("header", "false")
    .option("inferSchema", "true")
    .option("delimiter", "|")
    .csv("Files/feedback/passenger_feedback.txt")
)

# Print the number of rows and columns
print(f"Total rows: {df_feedback.count()}")
print(f"Total columns: {len(df_feedback.columns)}")

# Display the schema and the top 10 rows of the file
df_feedback.printSchema()
display(df_feedback.limit(10))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ##### 4.2 Check missing values

# CELL ********************

df_feedback.select([
    F.count(F.when(F.col(c).isNull(), c)).alias(c)
    for c in df_feedback.columns
]).show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ##### 4.3 Check the actual columns and duplicates

# CELL ********************

# Check columns name
print(df_feedback.columns)

# Check duplicates id considering the first column as column id.
df_feedback.groupBy("_c0") \
    .count() \
    .filter(F.col("count") > 1) \
    .show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Identify the date with the bad record
df_feedback.filter(
    F.col("_c1").isNull() |
    F.to_date(F.col("_c1")).isNull()
).show(truncate=False)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ##### 4.4 Rename and set data types

# CELL ********************

# Rename columns and set the data types
df_feedback_clean = (
    df_feedback
    .toDF("feedback_id", "feedback_date", "feedback_text")
    .withColumn("feedback_id", F.col("feedback_id").cast("int"))
    .withColumn("feedback_date", F.to_date("feedback_date", "yyyy-MM-dd"))
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ##### 4.5 Remove missing or empty feedback

# CELL ********************

df_feedback_clean = df_feedback_clean.filter(
    F.col("feedback_text").isNotNull() &
    (F.trim(F.col("feedback_text")) != "")
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ##### 4.6 Final check after cleaning

# CELL ********************

# print the number of rows
print("Rows:", df_feedback_clean.count())

# Print the schema
df_feedback_clean.printSchema()

# check for missing values
df_feedback_clean.select([
    F.count(F.when(F.col(c).isNull(), c)).alias(c)
    for c in df_feedback_clean.columns
]).show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Data Quality Findings
# 
# The Passenger Feedback dataset contains 181 passenger feedback records.
# 
# The quality assessment and cleaning identified:
# 
# - The pipe-delimited text file was parsed into three structured fields: feedback ID, feedback date and feedback text.
# - Column names were standardized and appropriate data types were applied.
# - No duplicate feedback identifiers were identified.
# - No missing or empty feedback text was identified.
# - One invalid date value (`bad-date`) was converted to NULL.
# - The associated feedback record was retained because its text remains valid and useful for passenger-experience analysis.

# MARKDOWN ********************

# ### Save to Silver Lakehouse

# CELL ********************

# Write Passenger Feedback to Silver
silver_path = "abfss://Airport_operations_analytics@onelake.dfs.fabric.microsoft.com/LH_Silver.Lakehouse/Tables"

df_feedback_clean.write.format("delta").mode("overwrite").save(f"{silver_path}/Tables/passenger_feedback")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ##### 
