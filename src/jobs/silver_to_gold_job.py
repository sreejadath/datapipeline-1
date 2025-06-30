import sys
import os
from pyspark.sql.functions import sha2, concat_ws, current_timestamp, lit, col, year, month, dayofmonth, hour, minute, avg, stddev, count, min as min_, max as max_,sum as sum_, expr
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, TimestampType, IntegerType, BooleanType, DateType
from src.logger.custom_logger import Logger
from src.utils import map_production_attributes
from typing import Dict, Any
from pathlib import Path
from datetime import datetime, timezone

# Add the project root to Python path
project_root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.insert(0, project_root)

class SilverToGoldTransformation:
    """
    Handles Bronze to Silver layer transformations for all energy data types.
    Applies data quality checks, cleansing, and standardization.
    """
    def __init__(self, spark, logger, config):
        self.spark = spark
        self.logger = logger
        self.config = config
        self.gold_path = config['delta_lake']['storage']['gold_path']

    def _create_gold_schemas(self):
        """Define schemas for Gold layer tables"""
        self.gold_dim_production_type_schema = StructType([
            StructField("production_type_id", StringType(), nullable=False),
            StructField("production_plant_name", StringType(), nullable=False),
            StructField("energy_category", StringType(), nullable=False),
            StructField("controllability_type", StringType(), nullable=False),
            StructField("description", StringType(), nullable=True),
            StructField("country", StringType(), nullable=False),
            StructField("effective_date", DateType(), nullable=False),
            StructField("expiry_date", DateType(), nullable=True),
            StructField("active_flag", BooleanType(), nullable=False),
            StructField("created_at", TimestampType(), nullable=False),
            StructField("updated_at", TimestampType(), nullable=True)
        ])

        self.gold_fact_power_schema = StructType([
            StructField("record_id", StringType(), nullable=False),
            StructField("year", IntegerType(), nullable=False),
            StructField("month", IntegerType(), nullable=False),
            StructField("day", IntegerType(), nullable=False),
            StructField("hour", IntegerType(), nullable=False),
            StructField("minute", IntegerType(), nullable=False),
            StructField("minute_interval_30", IntegerType(), nullable=False),
            StructField("timestamp_30min", TimestampType(), nullable=False),
            StructField("timestamp", TimestampType(), nullable=False),
            StructField("production_type_id", StringType(), nullable=False),
            StructField("electricity_produced", DoubleType(), nullable=False),
            StructField("electricity_price", DoubleType(), nullable=True),
            StructField("country", StringType(), nullable=False),
            StructField("created_at", TimestampType(), nullable=False),
            StructField("updated_at", TimestampType(), nullable=True)
        ])

        self.gold_fact_power_30min_agg_schema = StructType([
            StructField("record_id", StringType(), nullable=False),
            StructField("year", IntegerType(), nullable=False),
            StructField("month", IntegerType(), nullable=False),
            StructField("day", IntegerType(), nullable=False),
            StructField("hour", IntegerType(), nullable=False),
            StructField("minute", IntegerType(), nullable=False),
            StructField("minute_interval_30", IntegerType(), nullable=False),
            StructField("timestamp_30min", TimestampType(), nullable=False),
            StructField("timestamp", TimestampType(), nullable=False),
            StructField("production_type_id", StringType(), nullable=False),
            StructField("avg_electricity_produced", DoubleType(), nullable=True),
            StructField("total_electricity_produced", DoubleType(), nullable=True),
            StructField("min_electricity_produced", DoubleType(), nullable=True),
            StructField("max_electricity_produced", DoubleType(), nullable=True),
            StructField("production_volatility", DoubleType(), nullable=True),
            StructField("data_points_count", IntegerType(), nullable=True),
            StructField("avg_electricity_price", DoubleType(), nullable=True),
            StructField("country", StringType(), nullable=False),
            StructField("created_at", TimestampType(), nullable=False),
            StructField("updated_at", TimestampType(), nullable=True)
        ])

    def load_dim_production_type(self) -> None:
        """
        Loads the gold_dim_production_type dimension table from standardized silver tables.
        Extracts distinct production types from both silver_public_power and silver_installed_power,
        maps to gold attributes, and writes to the gold layer.
        """
        self.logger.info("Starting Silver to Gold load for dim_production_type")
        try:
            silver_public_path = self.config['delta_lake']['tables']['silver_public_power']
            silver_installed_path = self.config['delta_lake']['tables']['silver_installed_power']
            gold_path = self.config['delta_lake']['tables']['gold_dim_production_type']

            public_power_df = self.spark.read.parquet(silver_public_path)
            installed_power_df = self.spark.read.parquet(silver_installed_path)

            distinct_prod_types = (
                public_power_df.select("production_type", "country")
                .union(installed_power_df.select("production_type", "country"))
                .distinct()
            )

            from pyspark.sql.functions import udf
            attr_schema = StructType([
                StructField("production_plant_name", StringType()),
                StructField("energy_category", StringType()),
                StructField("controllability_type", StringType()),
                StructField("description", StringType())
            ])
            map_udf = udf(map_production_attributes, attr_schema)

            mapped_df = distinct_prod_types.withColumn("mapped", map_udf(col("production_type")))
            for field in attr_schema.fieldNames():
                mapped_df = mapped_df.withColumn(field, col(f"mapped.{field}"))

            gold_df = mapped_df.select(
                sha2(concat_ws("_", col("production_plant_name"), col("country")), 256).alias("production_type_id"),
                col("production_plant_name"),
                col("energy_category"),
                col("controllability_type"),
                col("description"),
                col("country"),
                current_timestamp().cast(DateType()).alias("effective_date"),
                lit(None).cast(DateType()).alias("expiry_date"),
                lit(True).alias("active_flag"),
                current_timestamp().alias("created_at"),
                lit(None).cast("timestamp").alias("updated_at")
            )

            gold_df.write.format("parquet").mode("overwrite").option("compression", "snappy").save(gold_path)
            self.logger.info(f"Loaded dim_production_type with {gold_df.count()} records")
        except Exception as e:
            self.logger.error(f"Error loading dim_production_type: {str(e)}")
            raise

    def load_gold_fact_power(self) -> None:
        """
        Loads the gold_fact_power table from silver_public_power, silver_price, and gold_dim_production_type.
        Joins production and price data, links to production_type_id, and writes to the gold layer.
        """
        self.logger.info("Starting loading gold_fact_power table")
        try:
            silver_public_path = self.config['delta_lake']['tables']['silver_public_power']
            silver_price_path = self.config['delta_lake']['tables']['silver_price']
            gold_dim_production_type_path = self.config['delta_lake']['tables']['gold_dim_production_type']
            gold_fact_power_path = self.config['delta_lake']['tables']['gold_fact_power']

            self.logger.info("Read the silver tables")
            public_power_df = self.spark.read.parquet(silver_public_path)
            price_df = self.spark.read.parquet(silver_price_path)
            dim_production_type_df = self.spark.read.parquet(gold_dim_production_type_path)

            self.logger.info("Starting loading gold_fact_power table")
            prodtype_map_df = dim_production_type_df.select(
                "production_plant_name", "country", "production_type_id"
            ).withColumnRenamed("country", "country_code")

            fact_df = public_power_df.join(
                prodtype_map_df,
                (public_power_df.production_type == prodtype_map_df.production_plant_name) &
                (public_power_df.country == prodtype_map_df.country_code),
                how="left"
            )

            price_df = price_df.withColumnRenamed("timestamp", "price_timestamp") \
                               .withColumnRenamed("electricity_price", "joined_electricity_price") \
                               .withColumnRenamed("country", "price_country")
            fact_df = fact_df.join(
                price_df,
                (fact_df.timestamp == price_df.price_timestamp) &
                (fact_df.country == price_df.price_country),
                how="left"
            )

            fact_df = fact_df.withColumn(
                "minute_interval_30",
                expr("CASE WHEN minute(timestamp) < 15 THEN 0 ELSE 30 END")
            )
            fact_df = fact_df.withColumn(
                "timestamp_30min",
                expr("""
                    CAST(
                        CONCAT(
                            CAST(year(timestamp) AS STRING), '-',
                            LPAD(CAST(month(timestamp) AS STRING),2,'0'), '-',
                            LPAD(CAST(day(timestamp) AS STRING),2,'0'), ' ',
                            LPAD(CAST(hour(timestamp) AS STRING),2,'0'), ':',
                            LPAD(CAST(
                                CASE WHEN minute(timestamp) < 15 THEN 0 ELSE 30 END
                            AS STRING),2,'0'), ':00'
                        ) AS TIMESTAMP
                    )
                """)
            )

            fact_df = fact_df.withColumn(
                "record_id",
                sha2(concat_ws("_",
                    fact_df["timestamp"].cast("string"),
                    fact_df["production_type_id"],
                    fact_df["country"]
                ), 256)
            )

            gold_fact_df = fact_df.select(
                "record_id",
                year("timestamp").alias("year"),
                month("timestamp").alias("month"),
                dayofmonth("timestamp").alias("day"),
                hour("timestamp").alias("hour"),
                minute("timestamp").alias("minute"),
                "minute_interval_30",
                "timestamp_30min",
                "timestamp",
                "production_type_id",
                col("electricity_generated").alias("electricity_produced"),
                col("joined_electricity_price").cast("double").alias("electricity_price"),
                col("country"),
                current_timestamp().alias("created_at"),
                current_timestamp().alias("updated_at")
            )

            gold_fact_df.write.format("parquet").mode("overwrite").option("compression", "snappy").save(gold_fact_power_path)
            self.logger.info(f"Gold fact_power table loaded successfully: {gold_fact_df.count()} records")
        except Exception as e:
            self.logger.error(f"Error loading gold_fact_power table: {str(e)}")
            raise

    def load_gold_fact_power_30min_agg(self) -> None:
        """
        Loads the gold_fact_power_30min_agg table by aggregating gold_fact_power at 30-minute intervals.
        Computes aggregates for electricity production and price, grouped by production_type_id, country, and 30-min interval.
        """
        self.logger.info("Starting loading gold_fact_power_30min_agg table")
        try:
            gold_fact_power_path = self.config['delta_lake']['tables']['gold_fact_power']
            gold_fact_power_30min_agg_path = self.config['delta_lake']['tables']['gold_fact_power_30min_agg']

            fact_df = self.spark.read.parquet(gold_fact_power_path)

            fact_df = fact_df.withColumn(
                "minute_interval_30",
                (minute("timestamp") >= 30).cast("int") * 30
            )
            fact_df = fact_df.withColumn(
                "timestamp_30min",
                (
                    (col("timestamp").cast("long") - (col("timestamp").cast("long") % 1800))
                ).cast("timestamp")
            )

            agg_df = (
                fact_df.groupBy(
                    "production_type_id", "country", "timestamp_30min"
                ).agg(
                    year("timestamp_30min").alias("year"),
                    month("timestamp_30min").alias("month"),
                    dayofmonth("timestamp_30min").alias("day"),
                    hour("timestamp_30min").alias("hour"),
                    minute("timestamp_30min").alias("minute"),
                    min_("minute_interval_30").alias("minute_interval_30"),
                    min_("timestamp").alias("timestamp"),
                    avg("electricity_produced").alias("avg_electricity_produced"),
                    sum_("electricity_produced").alias("total_electricity_produced"),
                    min_("electricity_produced").alias("min_electricity_produced"),
                    max_("electricity_produced").alias("max_electricity_produced"),
                    stddev("electricity_produced").alias("production_volatility"),
                    count("electricity_produced").alias("data_points_count"),
                    avg("electricity_price").alias("avg_electricity_price"),
                )
            )

            agg_df = agg_df.withColumn(
                "record_id",
                sha2(concat_ws("_",
                    col("production_type_id"),
                    col("country"),
                    col("timestamp_30min").cast("string")
                ), 256)
            )

            agg_df = agg_df.withColumn("created_at", current_timestamp())
            agg_df = agg_df.withColumn("updated_at", lit(None).cast("timestamp"))

            gold_cols = [
                "record_id", "year", "month", "day", "hour", "minute", "minute_interval_30",
                "timestamp_30min", "timestamp", "production_type_id",
                "avg_electricity_produced", "total_electricity_produced",
                "min_electricity_produced", "max_electricity_produced",
                "production_volatility", "data_points_count", "avg_electricity_price",
                "country", "created_at", "updated_at"
            ]
            gold_agg_df = agg_df.select(*gold_cols)

            gold_agg_df.write.format("parquet").mode("overwrite").option("compression", "snappy").save(gold_fact_power_30min_agg_path)
            self.logger.info(f"Gold fact_power_30min_agg table loaded successfully: {gold_agg_df.count()} records")
        except Exception as e:
            self.logger.error(f"Error loading gold_fact_power_30min_agg table: {str(e)}")
            raise

    def run_all_transformations(self) -> None:
        """Run all Silver to Gold transformations"""
        self.logger.info("Starting all Silver to Gold transformations")
        try:
            self._create_gold_schemas()
            self.load_dim_production_type()
            self.load_gold_fact_power()
            self.load_gold_fact_power_30min_agg()
            self.logger.info("All Silver to Gold transformations completed successfully")
        except Exception as e:
            self.logger.error(f"Error in Silver to Gold transformations: {str(e)}")
            raise

# Convenience functions for individual job execution
def run_dim_production_type(spark, logger, config):
    transformer = SilverToGoldTransformation(spark, logger, config)
    transformer.load_dim_production_type()

def run_fact_power(spark, logger, config):
    transformer = SilverToGoldTransformation(spark, logger, config)
    transformer.load_gold_fact_power()

def run_fact_power_30min_agg(spark, logger, config):
    transformer = SilverToGoldTransformation(spark, logger, config)
    transformer.load_gold_fact_power_30min_agg()

def run_all_silver_to_gold(spark, logger, config):
    transformer = SilverToGoldTransformation(spark, logger, config)
    transformer.run_all_transformations()
