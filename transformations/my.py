import dlt
from pyspark.sql.functions import (
    col,
    trim,
    sum,
    count
)

@dlt.table(
    name="sales_catalog.bronze.orders_bronze",
    comment="Raw orders data ingested via Lakeflow Connect",
    table_properties={"delta.columnMapping.mode": "name"}
)
def orders_bronze():
    return spark.read.table("sales_catalog.default.`2_m_sales_records`")

@dlt.table(
    name="sales_catalog.bronze.customers_bronze",
    comment="Raw customers data ingested via Auto Loader",
    table_properties={"delta.columnMapping.mode": "name"}
)
def customers_bronze():
    return (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("header", "true")
        .option("cloudFiles.schemaLocation", "/Volumes/sales_catalog/bronze/raw_customers/schema")
        .load("/Volumes/sales_catalog/bronze/raw_customers/data/")
    )

@dlt.table(
    name="sales_catalog.silver.sales_records_silver",
    comment="Cleaned and validated sales data",
    table_properties={"delta.columnMapping.mode": "name"}
)
@dlt.expect("valid_dates", "`Order Date` IS NOT NULL AND `Ship Date` IS NOT NULL")
@dlt.expect("positive_units", "`Units Sold` >= 0")
@dlt.expect("non_negative_profit", "`Total Profit` >= 0")
@dlt.expect("valid_country", "Country RLIKE '^[A-Za-z ]+$'")
def sales_records_silver():
    return (
        dlt.read("sales_catalog.bronze.orders_bronze")
        .withColumn("Region", trim(col("Region")))
        .withColumn("Country", trim(col("Country")))
        .withColumn("Item Type", trim(col("Item Type")))
        .withColumn("Sales Channel", trim(col("Sales Channel")))
        .withColumn("Order Priority", trim(col("Order Priority")))
        .withColumn("Total Profit", col("Total Profit").cast("double"))
    )

@dlt.table(
    name="sales_catalog.gold.daily_sales_summary",
    comment="Daily sales summary for reporting",
    table_properties={"delta.columnMapping.mode": "name"}
)
def daily_sales_summary():
    return (
        dlt.read("sales_catalog.silver.sales_records_silver")
        .groupBy("Order Date")
        .agg(
            sum("Total Revenue").alias("total_revenue"),
            sum("Total Cost").alias("total_cost"),
            sum("Total Profit").alias("total_profit"),
            sum("Units Sold").alias("units_sold"),
            count("Order ID").alias("total_orders")
        )
    )
@dlt.table(
    name="sales_catalog.silver.invalid_country_records",
    comment="Records failing country validation",
    table_properties={
        "delta.columnMapping.mode": "name"
    }   
)
def invalid_country_records():
    return (
        dlt.read("sales_catalog.bronze.orders_bronze")
        .filter("Country IS NOT NULL")
        .filter("Country NOT RLIKE '^[A-Za-z ]+$'")
    )