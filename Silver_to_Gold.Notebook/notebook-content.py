# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "e597456d-02ee-4a2a-82c9-a90c65d73a24",
# META       "default_lakehouse_name": "LH_Silver",
# META       "default_lakehouse_workspace_id": "a7eb2f42-6705-4658-9ff5-c11724fe1696",
# META       "known_lakehouses": [
# META         {
# META           "id": "f5d0b3fd-5294-4ba0-bc4c-4515d6e67262"
# META         },
# META         {
# META           "id": "e597456d-02ee-4a2a-82c9-a90c65d73a24"
# META         }
# META       ]
# META     }
# META   }
# META }

# MARKDOWN ********************

# # Dimensional modeling
# 
# ## Purpose
# 
# This notebook transforms validated Silver-layer datasets into an analytics-ready
# Gold dimensional model for airport operational and executive reporting.
# 
# The Gold layer follows a fact constellation architecture composed of shared
# conformed dimensions and multiple fact tables representing distinct airport
# business processes.
# 
# The model is designed to support Power BI reporting across:
# 
# - Flight operations and delays
# - Passenger traffic and security wait times
# - Baggage operations
# - Commercial performance
# - Sustainability
# - Passenger feedback
# - Operational KPI targets
# 
# ## Modeling Approach
# 
# The transformation follows dimensional modeling principles:
# 
# 1. Read validated Delta tables from the Silver Lakehouse.
# 2. Build shared conformed dimensions.
# 3. Generate standardized surrogate keys for dimension records.
# 4. Build fact tables at clearly defined business grains.
# 5. Replace business identifiers in fact relationships with dimension keys.
# 6. Validate uniqueness, referential integrity, row counts and fact-table grains.
# 7. Store the resulting dimensional model as Delta tables in the Gold Lakehouse.
# 
# The Gold model will serve as the analytical foundation for the semantic model
# and Power BI dashboards.


# MARKDOWN ********************

# # 1. Notebook Setup
# 
# This section initializes the Spark environment and defines the Silver source
# and Gold destination locations used throughout the notebook.

# CELL ********************

# Import required PySpark functions

from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import *

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Silver Lakehouse source path
silver_path = "abfss://Airport_operations_analytics@onelake.dfs.fabric.microsoft.com/LH_Silver.Lakehouse/Tables"

# Gold Lakehouse destination path
gold_path = "abfss://Airport_operations_analytics@onelake.dfs.fabric.microsoft.com/LH_Gold.Lakehouse/Tables"

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # 2. Build conformed dimensions
# 
# Conformed dimensions provide consistent descriptive attributes that can be
# shared across multiple airport business processes.
# 
# The Gold model contains five core dimensions:
# 
# - DimDate
# - DimAirline
# - DimGate
# - DimFlight
# - DimTerminalZone
# 
# Dimension records use standardized surrogate keys while preserving the original
# business identifiers from the source systems.

# MARKDOWN ********************

# #### 2.1 DimDate development

# CELL ********************

# Load the relevant Silver date

# Read date from Flights table
flights_silver = (
    spark.read.format("delta")
    .load(f"{silver_path}/Tables/Flights")
)

#Read date from passenger_flow
passenger_flow_silver = (
    spark.read.format("delta")
    .load(f"{silver_path}/Tables/passenger_flow")
)

# Read date from commercial_transactions
commercial_silver = (
    spark.read.format("delta")
    .load(f"{silver_path}/Tables/commercial_transactions")
)

# Read date from sustainability_daily
sustainability_silver = (
    spark.read.format("delta")
    .load(f"{silver_path}/Tables/sustainability_daily")
)

# Read date from passenger_feedback
feedback_silver = (
    spark.read.format("delta")
    .load(f"{silver_path}/Tables/passenger_feedback")
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Combine dates from the different business processes

all_dates = (
    flights_silver
        .select(F.to_date("scheduled_time").alias("date"))

    .union(
        passenger_flow_silver.select(F.col("date"))
    )

    .union(
        commercial_silver.select(
            F.to_date("timestamp").alias("date")
        )
    )

    .union(
        sustainability_silver.select(F.col("date"))
    )

    .union(
        feedback_silver.select(F.col("feedback_date").alias("date"))
    )

    .filter(F.col("date").isNotNull())
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Determine the full analytical date range

date_range = all_dates.agg(
    F.min("date").alias("min_date"),
    F.max("date").alias("max_date")
)

display(date_range)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Generate DimDate

# CELL ********************

# Generate one record for every date within the analytical period

date_bounds = date_range.first()

start_date = date_bounds["min_date"]
end_date = date_bounds["max_date"]

dim_date = (
    spark.sql(
        f"""
        SELECT explode(
            sequence(
                to_date('{start_date}'),
                to_date('{end_date}'),
                interval 1 day
            )
        ) AS date
        """
    )

    # YYYYMMDD serves as the Date dimension key
    .withColumn(
        "date_key",
        F.date_format("date", "yyyyMMdd").cast("int")
    )

    .withColumn("day", F.dayofmonth("date"))
    .withColumn("day_name", F.date_format("date", "EEEE"))
    .withColumn("week_of_year", F.weekofyear("date"))
    .withColumn("month", F.month("date"))
    .withColumn("month_name", F.date_format("date", "MMMM"))
    .withColumn("quarter", F.quarter("date"))
    .withColumn("year", F.year("date"))

    .withColumn(
        "is_weekend",
        F.dayofweek("date").isin([1, 7])
    )

    .select(
        "date_key",
        "date",
        "day",
        "day_name",
        "week_of_year",
        "month",
        "month_name",
        "quarter",
        "year",
        "is_weekend"
    )
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ##### Inspect DimDate

# CELL ********************

# Print the rows number of DimDate
print(f"DimDate rows: {dim_date.count()}")

# Print the schema
dim_date.printSchema()

# Display DimDate content
display(dim_date.orderBy("date"))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Validate steps of DimDate

# CELL ********************

# Check primary-key uniqueness

dim_date.groupBy("date_key") \
    .count() \
    .filter(F.col("count") > 1) \
    .show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Check for missing keys or dates

dim_date.filter(
    F.col("date_key").isNull() |
    F.col("date").isNull()
).show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Double-check the boundary
dim_date.select(
    F.min("date").alias("start_date"),
    F.max("date").alias("end_date")
).show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### DimDate development findings
# 
# - The date dimension was generated dynamically from the complete analytical
#   date range represented in the Silver layer.
# - Each calendar date has one unique `date_key` using the YYYYMMDD convention.
# - Calendar attributes including day, week, month, quarter and year were derived
#   for Power BI time-based analysis.
# - Weekend indicators were added to support operational comparisons.
# - No duplicate or missing date keys were identified.

# MARKDOWN ********************

# ### Save DimDate to Gold Lakehouse

# CELL ********************

# Write DimDate to Gold

dim_date.write.format("delta").mode("overwrite").save(f"{gold_path}/Tables/dim_date")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 2.2 DimAirline
# 
# DimAirline provides the standardized airline attributes used to analyze flight
# operations by carrier.
# 
# The dimension is built from the validated Airlines dataset in the Silver layer.
# A unique surrogate key using the `da` prefix is generated for each airline,
# while the original airline code is retained as the business identifier.

# MARKDOWN ********************

# ##### Load Airlines from Silver Lakehouse

# CELL ********************

# Load validated Airlines data from Silver

airlines_silver = (
    spark.read.format("delta")
    .load(f"{silver_path}/Tables/Airlines")
)

display(airlines_silver)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ##### Build DimAirline

# CELL ********************

# Define deterministic ordering for surrogate-key generation

airline_window = Window.orderBy("airline_code")

# Build DimAirline

dim_airline = (airlines_silver
    .withColumn(
        "airline_key",
        F.concat(
            F.lit("da"),
            F.lpad(
                F.row_number().over(airline_window).cast("string"),
                4,
                "0"
            )
        )
    )
    .select(
        "airline_key",
        "airline_code",
        "airline_name",
        "service_type"
    )
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ##### Validation Steps

# CELL ********************

# Check surrogate-key uniqueness
dim_airline.groupBy("airline_key") \
    .count() \
    .filter(F.col("count") > 1) \
    .show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Check airline_code uniqueness
dim_airline.groupBy("airline_code") \
    .count() \
    .filter(F.col("count") > 1) \
    .show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Print the number of rows
print("DimAirline rows:", dim_airline.count())

# Print the schema
dim_airline.printSchema()

# Display the dataset schema
display(dim_airline)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### DimAirline Findings
# 
# - Six validated airline records were used to create the dimension.
# - Each airline is uniquely identified by its original airline_code.
# - Surrogate keys were generated using the standardized da0001 format.
# - Airline name and service type were retained as descriptive attributes.
# - No duplicate airline keys or airline codes were identified.

# MARKDOWN ********************

# ### Save DimAirline to Gold Lakehouse

# CELL ********************

# Write DimAirline to Gold

dim_airline.write.format("delta").mode("overwrite").save(f"{gold_path}/Tables/dim_airline")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 2.3 DimGate
# 
# DimGate provides standardized gate attributes used to analyze airport operations
# by gate, terminal zone, and aircraft capacity classification.
# 
# The dimension is built from the validated Gates dataset in the Silver layer.
# A unique surrogate key using the `dg` prefix is generated for each gate, while
# the original gate ID is retained as the business identifier.

# MARKDOWN ********************

# ##### Load Gates from Silver Lakehouse

# CELL ********************

# Load validated Gates data from Silver

gates_silver = (
    spark.read.format("delta")
    .load(f"{silver_path}/Tables/Gates")
)

display(gates_silver)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ##### Build DimGate

# CELL ********************

# Define deterministic ordering for surrogate-key generation

gate_window = Window.orderBy("gate_id")

# Build DimGate

dim_gate = (
    gates_silver
    .withColumn(
        "gate_key",
        F.concat(
            F.lit("dg"),
            F.lpad(
                F.row_number().over(gate_window).cast("string"),
                4,
                "0"
            )
        )
    )
    .select(
        "gate_key",
        "gate_id",
        "terminal_zone",
        "capacity_class"
    )
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Add Unknown Gate member for flights with missing/unassigned gates

unknown_gate = spark.createDataFrame(
    [("dg0000", "UNKNOWN", "Unknown", "Unknown")],
    ["gate_key", "gate_id", "terminal_zone", "capacity_class"]
)

dim_gate = unknown_gate.unionByName(dim_gate)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ##### Validation steps

# CELL ********************

# Check surrogate-key uniqueness

dim_gate.groupBy("gate_key") \
    .count() \
    .filter(F.col("count") > 1) \
    .show()

# Check business-key uniqueness

dim_gate.groupBy("gate_id") \
    .count() \
    .filter(F.col("count") > 1) \
    .show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Print the number of rows
print("DimGate rows:", dim_gate.count())

# Print the dataset schema
dim_gate.printSchema()

# Display the dataset content 
display(dim_gate)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### DimGate findings
# 
# - 15 validated airport gates were used to create the dimension.
# - Each gate retains its original `gate_id` as the business identifier.
# - Surrogate keys were generated using the standardized `dg0001` format.
# - Terminal zone and aircraft capacity classification were retained as descriptive attributes.
# - No duplicate gate keys or gate IDs were identified.

# MARKDOWN ********************

# ### Save DimGate to Gold Lakehouse

# CELL ********************

# Write DimGate to Gold

dim_gate.write.format("delta").mode("overwrite").save(f"{gold_path}/Tables/dim_gate")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 2.4 DimFlight
# 
# DimFlight provides the descriptive flight attributes shared across flight-related
# business processes.
# 
# The dimension is built from the validated Flights dataset in the Silver layer.
# Each flight retains its original flight_id as the business identifier, while
# a surrogate key using the df prefix is generated for dimensional relationships.
# 
# Operational measures and status information are excluded from the dimension and
# will be stored in FactFlightOperations.

# MARKDOWN ********************

# ##### Load Flights from Silver Lakehouse

# CELL ********************

# Load validated Flights data from Silver

flights_silver = (
    spark.read.format("delta")
    .load(f"{silver_path}/Tables/Flights")
)

print("Silver Flights rows:", flights_silver.count())

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ##### Build DimFlight

# CELL ********************

# Define deterministic ordering for surrogate-key generation

flight_window = Window.orderBy("flight_id")

# Build DimFlight

dim_flight = (
    flights_silver
    .withColumn(
        "flight_key",
        F.concat(
            F.lit("df"),
            F.lpad(
                F.row_number().over(flight_window).cast("string"),
                4,
                "0"
            )
        )
    )
    .select(
        "flight_key",
        "flight_id",
        "flight_number",
        "origin",
        "destination"
    )
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ##### Validation steps

# CELL ********************

# Check surrogate-key uniqueness

dim_flight.groupBy("flight_key").count().filter(F.col("count") > 1).show()

# Check flight business-key uniqueness

dim_flight.groupBy("flight_id").count().filter(F.col("count") > 1).show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Print the number of rows of dim_flight
print("DimFlight rows:", dim_flight.count())

# Print dim_flight schema
dim_flight.printSchema()

# Display top 10 first rows of dim_flight
display(dim_flight.limit(10))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### DimFlight Findings
# 
# - 700 validated flight records were used to create the dimension.
# - Each flight retains its original flight_id as the business identifier.
# - Surrogate keys were generated using the standardized df0001 format.
# - Flight number, origin, and destination were retained as descriptive attributes.
# - Operational measures were excluded from the dimension and reserved for the flight operations fact table.
# - No duplicate flight keys or flight IDs were identified.

# MARKDOWN ********************

# ### Save to Gold Lakehouse

# CELL ********************

# Write DimFlight to Gold

dim_flight.write.format("delta").mode("overwrite").save(f"{gold_path}/Tables/dim_flight")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 2.5 DimTerminalZone
# 
# DimTerminalZone provides a shared representation of airport terminal zones across
# multiple business processes.
# 
# The dimension supports consistent analysis of passenger flow and commercial
# transactions by terminal area. A surrogate key using the `dt` prefix is generated
# for each unique terminal zone.

# MARKDOWN ********************

# ##### Build DimTerminalZone

# CELL ********************

# Extract unique terminal zones from the validated Gates dataset

terminal_zones = (
    gates_silver
    .select("terminal_zone")
    .filter(F.col("terminal_zone").isNotNull())
    .distinct()
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ##### Generate the surrogate keys

# CELL ********************

# Define deterministic ordering for surrogate-key generation

terminal_window = Window.orderBy("terminal_zone")

# Build DimTerminalZone

dim_terminal_zone = (
    terminal_zones
    .withColumn(
        "terminal_zone_key",
        F.concat(
            F.lit("dt"),
            F.lpad(
                F.row_number().over(terminal_window).cast("string"),
                4,
                "0"
            )
        )
    )
    .select(
        "terminal_zone_key",
        "terminal_zone"
    )
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ##### Validation steps

# CELL ********************

# Check surrogate-key uniqueness

dim_terminal_zone.groupBy("terminal_zone_key").count().filter(F.col("count") > 1).show()

# Check terminal-zone uniqueness

dim_terminal_zone.groupBy("terminal_zone").count().filter(F.col("count") > 1).show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Print the number of rows from dim_terminal_zone
print("DimTerminalZone rows:", dim_terminal_zone.count())

# Print the schema
dim_terminal_zone.printSchema()

# Display terminal zone content
display(dim_terminal_zone)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### DimTerminalZone Findings
# 
# - Three validated terminal zones were identified: Domestic, International, and Transborder.
# - Terminal zones were derived from the standardized airport gate reference data.
# - Surrogate keys were generated using the standardized dt0001 format.
# - Each terminal zone appears once in the dimension.
# - No duplicate or missing terminal-zone values were identified.

# MARKDOWN ********************

# ### Save DimTerminalZone to Gold Lakehouse

# CELL ********************

# Write DimTerminalZone to Gold

dim_terminal_zone.write.format("delta").mode("overwrite").save(f"{gold_path}/Tables/dim_terminal_zone")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 3.1 FactFlightOperations
# 
# FactFlightOperations represents the core airport flight operation process at
# one row per flight.
# 
# The fact table combines operational measures from the validated Flights dataset
# with surrogate keys from dim_flight, dim_airline, dim_gate and dim_date.
# 
# It supports analysis of flight volume, delays, cancellations, passenger traffic,
# load factor, gate operations, and on-time performance.

# MARKDOWN ********************

# ##### Build the fact table

# CELL ********************

# Build FactFlightOperations

fact_flight_operations = (
    flights_silver

    # Add Flight surrogate key
    .join(
        dim_flight.select("flight_key", "flight_id"),
        on="flight_id",
        how="left"
    )

    # Add Airline surrogate key
    .join(
        dim_airline.select("airline_key", "airline_code"),
        on="airline_code",
        how="left"
    )

    # Add Gate surrogate key
    .join(
        dim_gate.select("gate_key", "gate_id"),
        on="gate_id",
        how="left"
    )
    
    # Map any unmatched gate to dg0000
    .withColumn(
    "gate_key",
    F.coalesce(F.col("gate_key"), F.lit("dg0000"))
    )

    # Create date key from scheduled flight date
    .withColumn(
        "date_key",
        F.date_format(
            F.to_date("scheduled_time"),
            "yyyyMMdd"
        ).cast("int")
    )

    .select(
        "flight_key",
        "date_key",
        "airline_key",
        "gate_key",
        "direction",
        "traffic_type",
        "scheduled_time",
        "actual_time",
        "seat_capacity",
        "passenger_count",
        "status",
        "delay_minutes",
        "delay_reason",
        "gate_in_time",
        "gate_out_time"
    )
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ##### Validation Step

# CELL ********************

# Check missing dimension keys

fact_flight_operations.select(
    F.sum(F.col("flight_key").isNull().cast("int"))
        .alias("missing_flight_key"),

    F.sum(F.col("date_key").isNull().cast("int"))
        .alias("missing_date_key"),

    F.sum(F.col("airline_key").isNull().cast("int"))
        .alias("missing_airline_key"),

    F.sum(F.col("gate_key").isNull().cast("int"))
        .alias("missing_gate_key")
).show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Verify one fact record per flight

fact_flight_operations.groupBy("flight_key") \
    .count() \
    .filter(F.col("count") > 1) \
    .show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Check that every date key exists in DimDate

fact_flight_operations.join(
    dim_date.select("date_key"),
    on="date_key",
    how="left_anti"
).show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Print the number of rows of fact_flight_operations
print("FactFlightOperations rows:",
      fact_flight_operations.count())

# Print the table schema
fact_flight_operations.printSchema()

# Display the top 10 first fact tables
display(fact_flight_operations.limit(10))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### FactFlightOperations Findings
# 
# - FactFlightOperations was built at a grain of one record per flight.
# - 700 validated flight records were retained from the Silver layer.
# - Flight, airline, gate, and date dimension keys were successfully assigned.
# - No duplicate flight keys were identified.
# - All date, flight, and airline foreign-key relationships passed referential-integrity validation.
# - One flight (F0019) had no valid physical gate assignment in the Silver layer.
# - The missing gate relationship was mapped to the dg0000 Unknown Gate member in dim_gate rather than altering the validated Silver source.
# - No missing foreign keys remain in the Gold fact table.
# - Operational attributes and measures required for flight volume, passenger, delay, cancellation, load factor, and gate analysis were retained.

# MARKDOWN ********************

# ### Save to Gold Lakehouse

# CELL ********************

# Write FactFlightOperations to Gold

fact_flight_operations.write.format("delta").mode("overwrite").save(f"{gold_path}/Tables/fact_flight_operations")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 3.2 FactPassengerFlow
# 
# FactPassengerFlow represents passenger movement through airport terminal zones
# at an hourly level.
# 
# The fact table is built from the validated Passenger Flow dataset and linked to
# dim_date and dim_terminal_zone using their respective dimension keys.
# 
# The table supports analysis of passenger throughput, peak travel periods,
# terminal traffic distribution, traffic type and security wait times.
# 
# **Grain:** One record per date, hour, terminal zone and traffic type.

# MARKDOWN ********************

# ##### Load Passenger Flow from Silver Lakehouse

# CELL ********************

# Load validated Passenger Flow data from Silver

passenger_flow_silver = (
    spark.read.format("delta")
    .load(f"{silver_path}/Tables/passenger_flow")
)

print("Silver Passenger Flow rows:", passenger_flow_silver.count())

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ##### Build the fact table passenger_flow_silver

# CELL ********************

# aggregate any duplicate into one record grain of FactPassengerFlow since we define 
# one row per date * hour * terminal zone * traffic type.

passenger_flow_aggregated = (
    passenger_flow_silver
    .groupBy(
        "date",
        "hour",
        "terminal_zone",
        "traffic_type"
    )
    .agg(
        F.sum("passengers_processed").alias("passengers_processed"),

        (
            F.sum(
                F.col("avg_security_wait_min") *
                F.col("passengers_processed")
            )
            / F.sum("passengers_processed")
        ).alias("avg_security_wait_min")
    )
)

passenger_flow_window = Window.orderBy(
    "date",
    "hour",
    "terminal_zone",
    "traffic_type"
)

fact_passenger_flow = (
    passenger_flow_aggregated

    .withColumn(
        "date_key",
        F.date_format("date", "yyyyMMdd").cast("int")
    )

    .join(
        dim_terminal_zone.select(
            "terminal_zone_key",
            "terminal_zone"
        ),
        on="terminal_zone",
        how="left"
    )

    .withColumn(
        "passenger_flow_key",
        F.concat(
            F.lit("pf"),
            F.lpad(
                F.row_number().over(passenger_flow_window).cast("string"),
                4,
                "0"
            )
        )
    )

    .select(
        "passenger_flow_key",
        "date_key",
        "terminal_zone_key",
        "hour",
        "traffic_type",
        "passengers_processed",
        "avg_security_wait_min"
    )
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ##### Validation steps

# CELL ********************

# check foreign keys

fact_passenger_flow.select(
    F.sum(
        F.col("date_key").isNull().cast("int")
    ).alias("missing_date_key"),

    F.sum(
        F.col("terminal_zone_key").isNull().cast("int")
    ).alias("missing_terminal_zone_key")
).show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Check the fact key

fact_passenger_flow.groupBy("passenger_flow_key") \
    .count() \
    .filter(F.col("count") > 1) \
    .show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Check the uniqueness of the business grain

fact_passenger_flow.groupBy(
    "date_key",
    "hour",
    "terminal_zone_key",
    "traffic_type"
).count() \
 .filter(F.col("count") > 1) \
 .show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### FactPassengerFlow findings
# 
# - FactPassengerFlow was built at a grain of one record per date, hour, terminal zone, and traffic type.
# - The fact table was linked to dim_date and dim_terminal_zone using their respective dimension keys.
# - Multiple source observations sharing the same business grain were aggregated into a single Gold record.
# - Passenger counts were aggregated using the total passengers processed.
# - Average security wait time was calculated using a passenger-weighted average to preserve the relative contribution of each observation.
# - No duplicate business-grain combinations remain after aggregation.
# - All date and terminal-zone foreign-key relationships passed referential-integrity validation.

# MARKDOWN ********************

# ### Save the fact table FactPassengerFlow to Gold Lakehouse

# CELL ********************

# Write FactPassengerFlow to Gold

fact_passenger_flow.write.format("delta").mode("overwrite").save(f"{gold_path}/Tables/fact_passenger_flow")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 3.3 FactBaggageOperations
# 
# FactBaggageOperations represents baggage-handling performance for airport flights.
# 
# The fact table is built from the validated Baggage Events dataset and linked to
# dim_flight using the flight business identifier. Since the baggage source does not
# contain a date, the associated flight's scheduled date is used to establish the
# relationship with dim_date.
# 
# The table supports analysis of baggage volume, mishandled baggage, first-bag
# delivery time, and last-bag delivery time.
# 
# **Grain:** One record per baggage event associated with a flight.

# MARKDOWN ********************

# ##### Load Baggage Events from Silver Lakehouse

# CELL ********************

# Load validated Baggage Events from Silver

baggage_silver = (
    spark.read.format("delta")
    .load(f"{silver_path}/Tables/baggage_events")
)

print("Silver Baggage Events rows:", baggage_silver.count())
baggage_silver.printSchema()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ##### Build FactBaggageOperations

# CELL ********************

# Prepare flight reference data for dimension and date relationships

flight_reference = (
    flights_silver
    .select(
        "flight_id",
        F.to_date("scheduled_time").alias("flight_date")
    )
)

# Build FactBaggageOperations

fact_baggage_operations = (
    baggage_silver

    # Retrieve the flight date
    .join(
        flight_reference,
        on="flight_id",
        how="left"
    )

    # Add DimFlight surrogate key
    .join(
        dim_flight.select("flight_key", "flight_id"),
        on="flight_id",
        how="left"
    )

    # Generate DimDate key from the associated flight
    .withColumn(
        "date_key",
        F.date_format("flight_date", "yyyyMMdd").cast("int")
    )

    .select(
        "bag_event_id",
        "flight_key",
        "date_key",
        "bags_processed",
        "mishandled_bags",
        "first_bag_min",
        "last_bag_min"
    )
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Validation steps

# CELL ********************

# Check the primary business-event grain
# Verify one record per baggage event

fact_baggage_operations.groupBy("bag_event_id") \
    .count() \
    .filter(F.col("count") > 1) \
    .show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Check missing dimensional relationships

fact_baggage_operations.select(
    F.sum(
        F.col("flight_key").isNull().cast("int")
    ).alias("missing_flight_key"),

    F.sum(
        F.col("date_key").isNull().cast("int")
    ).alias("missing_date_key")
).show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Verify that every baggage date exists in DimDate

fact_baggage_operations.join(
    dim_date.select("date_key"),
    on="date_key",
    how="left_anti"
).show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Print the number of rows from FactBaggageOperations
print("FactBaggageOperations rows:", fact_baggage_operations.count())

# Print the schema
fact_baggage_operations.printSchema()

# Print the top 10 first records of the fact table
display(fact_baggage_operations.limit(10))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### FactBaggageOperations Findings
# 
# - FactBaggageOperations was built at a grain of one record per baggage event associated with a flight.
# - 300 validated baggage-event records were retained from the Silver layer.
# - Each baggage event is uniquely identified by bag_event_id.
# - Baggage events were successfully linked to DimFlight using flight_key.
# - Since the source baggage dataset does not contain a date, date_key was derived from the scheduled date of the associated flight.
# - No duplicate baggage-event IDs were identified.
# - No missing flight or date foreign keys were identified.
# - All date relationships passed referential-integrity validation.
# - Baggage measures required for volume, mishandling, first-bag delivery, and last-bag delivery analysis were retained.

# MARKDOWN ********************

# ### Save the dataset to the Gold Lakehouse

# CELL ********************

# Write FactBaggageOperations to Gold

fact_baggage_operations.write.format("delta").mode("overwrite").save(f"{gold_path}/Tables/fact_baggage_operations")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 3.4 FactCommercialTransactions
# 
# FactCommercialTransactions represents commercial revenue activity across airport
# terminal zones.
# 
# The fact table is built from the validated Commercial Transactions dataset and
# linked to dim_date and dim_terminal_zone using their respective dimension keys.
# 
# The table supports analysis of total revenue, transaction volume, average
# transaction value, revenue category, payment method, and commercial performance
# by terminal zone.
# 
# Negative transaction amounts are retained because they represent valid refunds
# or financial adjustments.
# 
# **Grain:** One record per commercial transaction.

# MARKDOWN ********************

# ##### Load Commercial Transactions from Silver Lakehouse

# CELL ********************

# Load validated Commercial Transactions from Silver

commercial_silver = (
    spark.read.format("delta")
    .load(f"{silver_path}/Tables/commercial_transactions")
)

# Confirm the data integrity loaded from Silver Lakehouse
print("Silver Commercial Transactions rows:", commercial_silver.count())

# Print the schema
commercial_silver.printSchema()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ##### Build the fact table

# CELL ********************

# Build FactCommercialTransactions

fact_commercial_transactions = (
    commercial_silver

    # Create Date dimension key from transaction timestamp
    .withColumn(
        "date_key",
        F.date_format(
            F.to_date("timestamp"),
            "yyyyMMdd"
        ).cast("int")
    )

    # Add Terminal Zone dimension key
    .join(
        dim_terminal_zone.select(
            "terminal_zone_key",
            "terminal_zone"
        ),
        on="terminal_zone",
        how="left"
    )

    .select(
        "transaction_id",
        "date_key",
        "terminal_zone_key",
        "timestamp",
        "revenue_category",
        "payment_method",
        "amount_cad"
    )
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# #### Validation steps

# CELL ********************

# Verify one record per commercial transaction

fact_commercial_transactions.groupBy("transaction_id") \
    .count() \
    .filter(F.col("count") > 1) \
    .show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Check foreign keys is not null
fact_commercial_transactions.select(
    F.sum(
        F.col("date_key").isNull().cast("int")
    ).alias("missing_date_key"),

    F.sum(
        F.col("terminal_zone_key").isNull().cast("int")
    ).alias("missing_terminal_zone_key")
).show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# validate that every date_key exists in DimDate
fact_commercial_transactions.join(
    dim_date.select("date_key"),
    on="date_key",
    how="left_anti"
).show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Print the number of rows from fact_commercial_transactions
print("FactCommercialTransactions rows:", fact_commercial_transactions.count())

# Print the schema
fact_commercial_transactions.printSchema()

# Display the top 10 first row from the fact table
display(fact_commercial_transactions.limit(10))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### FactCommercialTransactions Findings
# 
# - FactCommercialTransactions was built at a grain of one record per commercial transaction.
# - 500 validated transaction records were retained from the Silver layer.
# - Each transaction is uniquely identified by `transaction_id`.
# - Transactions were successfully linked to DimDate and DimTerminalZone.
# - No duplicate transaction IDs were identified.
# - No missing date or terminal-zone foreign keys were identified.
# - All date relationships passed referential-integrity validation.
# - Negative transaction amounts were retained as valid refunds or financial adjustments.
# - The existing missing transaction amount was preserved as NULL because no reliable value is available for imputation.
# - Revenue category, payment method, transaction timestamp, and transaction amount were retained to support commercial-performance analysis.

# MARKDOWN ********************

# ### Save FactCommercialTransactions to the Gold Lakehouse

# CELL ********************

# Write FactCommercialTransactions to Gold

fact_commercial_transactions.write.format("delta").mode("overwrite").save(f"{gold_path}/Tables/fact_commercial_transactions")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 3.5 FactSustainabilityDaily
# 
# FactSustainabilityDaily represents daily airport environmental and resource
# consumption performance.
# 
# The fact table is built from the validated Sustainability Daily dataset and
# linked to DimDate using the date dimension key.
# 
# The table supports analysis of electricity consumption, water consumption,
# waste generation, and estimated CO2e emissions over time.
# 
# **Grain:** One record per calendar date.

# MARKDOWN ********************

# ##### Load Sustainability data from Silver

# CELL ********************

# Load validated Sustainability Daily data from Silver

sustainability_silver = (
    spark.read.format("delta")
    .load(f"{silver_path}/Tables/sustainability_daily")
)

# Check the dataset integrity 
print("Silver Sustainability rows:", sustainability_silver.count())

# Print the schema
sustainability_silver.printSchema()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ##### Build the fact table

# CELL ********************

# Build FactSustainabilityDaily

fact_sustainability_daily = (
    sustainability_silver

    # Create DimDate foreign key
    .withColumn(
        "date_key",
        F.date_format("date", "yyyyMMdd").cast("int")
    )

    .select(
        "date_key",
        "electricity_kwh",
        "water_m3",
        "waste_tonnes",
        "estimated_co2e_tonnes"
    )
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ##### Validation steps

# CELL ********************

# Verify one record per calendar date

fact_sustainability_daily.groupBy("date_key") \
    .count() \
    .filter(F.col("count") > 1) \
    .show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Check for missing Date dimension keys

fact_sustainability_daily.select(
    F.sum(
        F.col("date_key").isNull().cast("int")
    ).alias("missing_date_key")
).show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Verify every sustainability date exists in dim_date

fact_sustainability_daily.join(
    dim_date.select("date_key"),
    on="date_key",
    how="left_anti"
).show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Print the number of rows from fact_sustainability_daily
print("FactSustainabilityDaily rows:", fact_sustainability_daily.count())

# Print the schema of the dataset
fact_sustainability_daily.printSchema()

# Display the top 10 first records from the dataset
display(fact_sustainability_daily.limit(10))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### FactSustainabilityDaily findings
# 
# - FactSustainabilityDaily was built at a grain of one record per calendar date.
# - 31 validated daily sustainability records were retained from the Silver layer.
# - Each record was successfully linked to DimDate using `date_key`.
# - No duplicate date keys were identified.
# - No missing or unmatched date foreign keys were identified.
# - The water consumption missing value had already been handled during the Silver cleaning stage, so no additional imputation was required in Gold.
# - Electricity consumption, water consumption, waste generation, and estimated CO2e emissions were retained to support sustainability performance analysis.

# MARKDOWN ********************

# ##### Save the dataset to Gold Lakehouse

# CELL ********************

# Write FactSustainabilityDaily to Gold

fact_sustainability_daily.write.format("delta").mode("overwrite").save(f"{gold_path}/Tables/fact_sustainability_daily")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 3.6 FactPassengerFeedback
# 
# FactPassengerFeedback represents individual passenger comments collected about
# the airport experience.
# 
# The fact table is built from the validated Passenger Feedback dataset and linked
# to DimDate where a valid feedback date is available.
# 
# The table preserves the original passenger feedback text for downstream passenger
# experience analysis and potential future text analytics, such as sentiment and
# feedback-category classification.
# 
# **Grain:** One record per passenger feedback submission.

# MARKDOWN ********************

# ##### Load Passenger Feedback from Silver Lakehouse

# CELL ********************

# Load validated Passenger Feedback data from Silver

feedback_silver = (
    spark.read.format("delta")
    .load(f"{silver_path}/Tables/passenger_feedback")
)

print("Silver Passenger Feedback rows:", feedback_silver.count())
feedback_silver.printSchema()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ##### Build FactPassengerFeedback

# CELL ********************

# Build FactPassengerFeedback

fact_passenger_feedback = (
    feedback_silver

    # Create DimDate foreign key when a valid feedback date exists
    .withColumn(
        "date_key",
        F.date_format("feedback_date", "yyyyMMdd").cast("int")
    )

    .select(
        "feedback_id",
        "date_key",
        "feedback_text"
    )
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ##### Validation steps

# CELL ********************

# Verify one record per feedback submission

fact_passenger_feedback.groupBy("feedback_id") \
    .count() \
    .filter(F.col("count") > 1) \
    .show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Check the date relationship

fact_passenger_feedback.select(
    F.sum(
        F.col("date_key").isNull().cast("int")
    ).alias("missing_date_key")
).show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# confirm that all non-null date keys exist in dim_date

fact_passenger_feedback \
    .filter(F.col("date_key").isNotNull()) \
    .join(
        dim_date.select("date_key"),
        on="date_key",
        how="left_anti"
    ) \
    .show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Print the dataset number of rows
print("FactPassengerFeedback rows:", fact_passenger_feedback.count())

# Print the schema
fact_passenger_feedback.printSchema()

# Display the top 10 first rows
display(fact_passenger_feedback.limit(10))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### FactPassengerFeedback findings
# 
# - FactPassengerFeedback was built at a grain of one record per passenger feedback submission.
# - 181 validated passenger feedback records were retained from the Silver layer.
# - Each feedback record is uniquely identified by `feedback_id`.
# - No duplicate feedback IDs were identified.
# - Valid feedback dates were successfully linked to DimDate using `date_key`.
# - One record has a NULL `date_key` because its original source date was invalid (`bad-date`).
# - The record was retained because its feedback text remains valid and analytically useful.
# - No artificial date was assigned, avoiding the introduction of inaccurate temporal information.
# - All non-null date keys passed referential-integrity validation.
# - Passenger feedback text was retained for reporting and potential future text analytics.

# MARKDOWN ********************

# ### Save FactPassengerFeedback to Gold Lakehouse

# CELL ********************

# Write FactPassengerFeedback to Gold

fact_passenger_feedback.write.format("delta").mode("overwrite").save(f"{gold_path}/Tables/fact_passenger_feedback")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # 4. Operational Targets
# 
# OperationalTargets stores the business performance thresholds used to evaluate
# actual airport KPIs.
# 
# The table is built from the validated Operational Targets dataset in the Silver
# layer. It provides the benchmark values required to compare actual performance
# against operational objectives in the Power BI semantic model.
# 
# Targets include an effective start and end date, allowing KPI thresholds to
# change over time.
# 
# **Grain:** One record per KPI target and effective period.

# MARKDOWN ********************

# ##### Load the dataset from Silver Lakehouse

# CELL ********************

# Load validated Operational Targets from Silver

operational_targets_silver = (
    spark.read.format("delta")
    .load(f"{silver_path}/Tables/operational_targets")
)

print("Operational Targets rows:", operational_targets_silver.count())
operational_targets_silver.printSchema()

display(operational_targets_silver)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ##### Build OperationalTargets

# CELL ********************

# Define deterministic ordering for target-key generation

target_window = Window.orderBy(
    "kpi_name",
    "effective_start_date"
)

# Build OperationalTargets

operational_targets = (
    operational_targets_silver

    .withColumn(
        "target_key",
        F.concat(
            F.lit("ot"),
            F.lpad(
                F.row_number().over(target_window).cast("string"),
                4,
                "0"
            )
        )
    )

    .select(
        "target_key",
        "kpi_name",
        "kpi_code",
        "business_area",
        "unit",
        "target_value",
        "target_direction",
        "effective_start_date",
        "effective_end_date"
    )
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ##### Validation steps

# CELL ********************

# Check target-key uniqueness

operational_targets.groupBy("target_key") \
    .count() \
    .filter(F.col("count") > 1) \
    .show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Check for duplicate KPI targets within the same effective period

operational_targets.groupBy(
    "kpi_name",
    "effective_start_date",
    "effective_end_date"
).count() \
 .filter(F.col("count") > 1) \
 .show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Print the number of rows
print("OperationalTargets rows:", operational_targets.count())

# Print the schema
operational_targets.printSchema()

# Display the fact table content
display(operational_targets)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### OperationalTargets Findings
# 
# - OperationalTargets was built at a grain of one record per KPI target and effective period.
# - Seven validated KPI target records were retained from the Silver layer.
# - Each target was assigned a unique surrogate key using the `ot0001` convention.
# - No duplicate target keys or duplicate KPI/effective-period combinations were identified.
# - Target values were retained as numeric values for comparison with actual KPI performance.
# - Effective start and end dates were retained to support time-dependent targets.
# - NULL effective end dates are preserved for targets that remain active.
# - Target direction was retained to determine whether KPI performance should be evaluated as above or below the defined threshold.

# MARKDOWN ********************

# ##### Save to Gold Lakehouse

# CELL ********************

# Write OperationalTargets to Gold

operational_targets.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(f"{gold_path}/Tables/operational_targets")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # 5. Gold Model Validation and Conclusion
# 
# The Gold dimensional model was validated progressively as each dimension, fact,
# and reference table was created.
# 
# The validation confirmed:
# 
# - Dimension keys are unique and consistent.
# - Each fact table respects its defined business grain.
# - Foreign-key relationships between facts and dimensions are valid.
# - Passenger Flow observations were aggregated to the defined hourly business grain.
# - The missing flight gate was handled through the `dg0000` Unknown Gate member.
# - The passenger feedback record with an invalid source date was retained with a NULL date key.
# - Operational targets were successfully prepared for KPI benchmarking.
# 
# The resulting Gold layer consists of shared conformed dimensions, business-process
# fact tables, and operational targets designed to support airport executive and
# operational reporting.
