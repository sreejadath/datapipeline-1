import sys
import os

# Add the project root to Python path
project_root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.insert(0, project_root)

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import (
    col, year, month, dayofmonth, hour, minute, 
    when, isnan, isnull, regexp_replace, trim, upper,
    current_timestamp, lit, coalesce, avg, stddev,
    row_number, desc, asc
)
from pyspark.sql.window import Window
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, TimestampType, IntegerType, BooleanType
from src.logger.custom_logger import Logger
from typing import Dict, Any
from pathlib import Path
from datetime import datetime, timezone


class BronzeToSilverTransformation:
    """
    Handles Bronze to Silver layer transformations for all energy data types.
    Applies data quality checks, cleansing, and standardization.
    """
    
    def __init__(self, spark: SparkSession, logger: Logger, config: Dict[str, Any]):
        self.spark = spark
        self.logger = logger
        self.config = config
        self.silver_path = config['delta_lake']['storage']['silver_path']
        
    def _create_silver_schemas(self):
        """Define schemas for Silver layer tables"""
        
        # Silver Public Power Schema
        self.silver_public_power_schema = StructType([
            StructField("timestamp", TimestampType(), False),
            StructField("production_type", StringType(), False),
            StructField("electricity_generated", DoubleType(), True),
            StructField("year", IntegerType(), False),
            StructField("month", IntegerType(), False),
            StructField("day", IntegerType(), False),
            StructField("hour", IntegerType(), False),
            StructField("country", StringType(), False),
            StructField("unit", StringType(), True),
            StructField("source_system", StringType(), False),
            StructField("ingestion_timestamp", TimestampType(), False),
            StructField("processing_timestamp", TimestampType(), False),
            StructField("data_quality_score", DoubleType(), True),
            StructField("is_anomaly", BooleanType(), False),
            StructField("is_valid", BooleanType(), False)
        ])
        
        # Silver Price Schema
        self.silver_price_schema = StructType([
            StructField("timestamp", TimestampType(), False),
            StructField("electricity_price", DoubleType(), True),
            StructField("currency", StringType(), False),
            StructField("year", IntegerType(), False),
            StructField("month", IntegerType(), False),
            StructField("day", IntegerType(), False),
            StructField("country", StringType(), False),
            StructField("unit", StringType(), True),
            StructField("source_system", StringType(), False),
            StructField("ingestion_timestamp", TimestampType(), False),
            StructField("processing_timestamp", TimestampType(), False),
            StructField("data_quality_score", DoubleType(), True),
            StructField("is_anomaly", BooleanType(), False),
            StructField("is_valid", BooleanType(), False)
        ])
        
        # Silver Installed Power Schema
        self.silver_installed_power_schema = StructType([
            StructField("timestamp", TimestampType(), False),
            StructField("production_type", StringType(), False),
            StructField("installed_capacity", DoubleType(), True),
            StructField("year", IntegerType(), False),
            StructField("month", IntegerType(), False),
            StructField("country", StringType(), False),
            StructField("unit", StringType(), True),
            StructField("source_system", StringType(), False),
            StructField("ingestion_timestamp", TimestampType(), False),
            StructField("processing_timestamp", TimestampType(), False),
            StructField("data_quality_score", DoubleType(), True),
            StructField("is_valid", BooleanType(), False)
        ])

    def _apply_data_quality_checks(self, df: DataFrame, data_type: str) -> DataFrame:
        """Apply comprehensive data quality checks and scoring"""
        
        self.logger.info(f"Applying data quality checks for {data_type}")
        
        if data_type == "public_power":
            # Public Power specific quality checks
            df_with_quality = df.withColumn(
                "data_quality_score",
                when(col("electricity_generated").isNull(), 0.0)
                .when(col("electricity_generated") < 0, 0.3)  # Negative values are suspicious
                .when(col("electricity_generated") > 100000, 0.5)  # Very high values might be outliers
                .otherwise(1.0)
            ).withColumn(
                "is_valid",
                when(col("timestamp").isNull(), False)
                .when(col("production_type").isNull(), False)
                .when(col("electricity_generated").isNull(), False)
                .when(col("electricity_generated") < 0, False)
                .otherwise(True)
            )
            
        elif data_type == "price":
            # Price specific quality checks
            df_with_quality = df.withColumn(
                "data_quality_score",
                when(col("electricity_price").isNull(), 0.0)
                .when(col("electricity_price") < 0, 0.1)  # Negative prices can occur but are unusual
                .when(col("electricity_price") > 1000, 0.5)  # Very high prices might be outliers
                .otherwise(1.0)
            ).withColumn(
                "is_valid",
                when(col("timestamp").isNull(), False)
                .when(col("electricity_price").isNull(), False)
                .otherwise(True)
            )
        elif  data_type == "installed_power":
            # Installed Power specific quality checks
            df_with_quality = df.withColumn(
                "data_quality_score",
                when(col("installed_power").isNull(), 0.0)
                .when(col("installed_power") < 0, 0.0)  # Cannot have negative capacity
                .when(col("installed_power") > 1000000, 0.5)  # Very high capacity might be outliers
                .otherwise(1.0)
            ).withColumn(
                "is_valid",
                when(col("timestamp").isNull(), False)
                .when(col("installed_power").isNull(), False)
                .when(col("installed_power").isNull(), False)
                .when(col("installed_power") < 0, False)
                .otherwise(True)
            )
        return df_with_quality

    def _detect_anomalies(self, df: DataFrame, value_column: str) -> DataFrame:
        """Detect statistical anomalies using Z-score method"""
        
        self.logger.info(f"Detecting anomalies in {value_column}")
        
        # Calculate statistics for anomaly detection
        stats = df.select(
            avg(col(value_column)).alias("mean_value"),
            stddev(col(value_column)).alias("stddev_value")
        ).collect()[0]
        
        mean_val = float(stats["mean_value"] or 0.0)
        stddev_val = float(stats["stddev_value"] or 0.0)
        
        # Mark anomalies (values > 3 standard deviations from mean)
        if stddev_val > 0:
            df_with_anomalies = df.withColumn(
                "is_anomaly",
                when(
                    (col(value_column) > (mean_val + 3 * stddev_val)) |
                    (col(value_column) < (mean_val - 3 * stddev_val)),
                    True
                ).otherwise(False)
            )
        else:
            df_with_anomalies = df.withColumn("is_anomaly", lit(False))
            
        return df_with_anomalies

    def _standardize_production_types(self, df: DataFrame) -> DataFrame:
        """Standardize production type names"""
        
        self.logger.info("Standardizing production type names")
        
        # Define production type mapping for standardization
        production_type_mapping = {
            "wind": "Wind",
            "solar": "Solar", 
            "nuclear": "Nuclear",
            "fossil": "Fossil",
            "hydro": "Hydro",
            "biomass": "Biomass",
            "geothermal": "Geothermal",
            "wind_offshore": "Wind Offshore",
            "wind_onshore": "Wind Onshore",
            "pv": "Solar PV",
            "run_of_river": "Run of River",
            "pumped_storage": "Pumped Storage"
        }
        
        # Apply standardization
        df_standardized = df
        for old_name, new_name in production_type_mapping.items():
            df_standardized = df_standardized.withColumn(
                "production_type",
                when(upper(trim(col("production_type"))).contains(old_name.upper()), new_name)
                .otherwise(col("production_type"))
            )
            
        return df_standardized

    def _deduplicate_records(self, df: DataFrame, partition_columns: list, order_column: str = "ingestion_timestamp") -> DataFrame:
        """Remove duplicate records keeping the latest one"""
        
        self.logger.info("Removing duplicate records")
        
        window_spec = Window.partitionBy(*partition_columns).orderBy(desc(order_column))
        
        df_deduped = df.withColumn(
            "row_number", row_number().over(window_spec)
        ).filter(col("row_number") == 1).drop("row_number")
        
        return df_deduped

    def transform_public_power_to_silver(self, country: str = "de") -> None:
        """Transform public power data from Bronze to Silver"""
        
        self.logger.info("Starting public power Bronze to Silver transformation")
        
        try:
            # Read from Bronze layer
            bronze_path = f"{self.config['delta_lake']['tables']['bronze_public_power']}"
            silver_path = f"{self.config['delta_lake']['tables']['silver_public_power']}"
            
            self.logger.info(f"Reading from Bronze path: {bronze_path}")
            bronze_df = self.spark.read.format("parquet").load(bronze_path)
            
            if bronze_df.count() == 0:
                self.logger.warning("No data found in Bronze public_power table")
                return
            
            # Apply transformations
            silver_df = bronze_df.select(
                col("timestamp"),
                col("production_type"),
                col("electricity_generated"),
                year(col("timestamp")).alias("year"),
                month(col("timestamp")).alias("month"), 
                dayofmonth(col("timestamp")).alias("day"),
                hour(col("timestamp")).alias("hour"),
                lit(country).alias("country"),
                lit("MW").alias("unit"),
                lit("energy_charts_api").alias("source_system"),
                col("ingestion_date").alias("ingestion_timestamp"),
                current_timestamp().alias("processing_timestamp")
            )
            
            # Standardize production types
            silver_df = self._standardize_production_types(silver_df)
            
            # Apply data quality checks
            silver_df = self._apply_data_quality_checks(silver_df, "public_power")
            
            # Detect anomalies
            silver_df = self._detect_anomalies(silver_df, "electricity_generated")
            
            # Remove duplicates
            partition_cols = ["timestamp", "production_type", "country"]
            silver_df = self._deduplicate_records(silver_df, partition_cols)
            
            # Create Silver directory if it doesn't exist
            Path(silver_path).parent.mkdir(parents=True, exist_ok=True)
            
            # Write to Silver layer
            self.logger.info(f"Writing to Silver path: {silver_path}")
            """(silver_df.write
             .format("delta")
             .mode("overwrite")
             .option("overwriteSchema", "true")
             .option("delta.enableChangeDataFeed", "true")
             .partitionBy("year", "month", "production_type")
             .save(silver_path)) """

            (silver_df.write
                .format("parquet")
                .mode('append')
                .option("compression", "snappy")
                .save(silver_path))
            
            # Log transformation stats
            total_records = silver_df.count()
            valid_records = silver_df.filter(col("is_valid") == True).count()
            invalid_records = total_records - valid_records
            
            self.logger.info(f"Public Power transformation completed:")
            self.logger.info(f"  Total records: {total_records}")
            self.logger.info(f"  Valid records: {valid_records}")
            self.logger.info(f"  Invalid records: {invalid_records}")
            
        except Exception as e:
            self.logger.error(f"Error in public power transformation: {str(e)}")
            raise

    def transform_price_to_silver(self, country: str = "de") -> None:
        """Transform price data from Bronze to Silver"""
        
        self.logger.info("Starting price Bronze to Silver transformation")
        
        try:
            # Read from Bronze layer
            bronze_path = f"{self.config['delta_lake']['tables']['bronze_price']}"
            silver_path = f"{self.config['delta_lake']['tables']['silver_price']}"
            
            self.logger.info(f"Reading from Bronze path: {bronze_path}")
            bronze_df = self.spark.read.format("parquet").load(bronze_path)
            
            if bronze_df.count() == 0:
                self.logger.warning("No data found in Bronze price table")
                return
            
            # Apply transformations
            silver_df = bronze_df.select(
                col("timestamp"),
                col("electricity_price"),
                lit("EUR").alias("currency"),
                lit("EPEX_SPOT").alias("market"),
                year(col("timestamp")).alias("year"),
                month(col("timestamp")).alias("month"),
                dayofmonth(col("timestamp")).alias("day"),
                lit(country).alias("country"),
                coalesce(col("electricity_unit"), lit("EUR/MWh")).alias("unit"),
                lit("energy_charts_api").alias("source_system"),
                col("ingestion_date").alias("ingestion_timestamp"),
                current_timestamp().alias("processing_timestamp")
            )
            
            # Apply data quality checks
            silver_df = self._apply_data_quality_checks(silver_df, "price")
            
            # Detect anomalies
            silver_df = self._detect_anomalies(silver_df, "electricity_price")
            
            # Remove duplicates
            partition_cols = ["timestamp", "country"]
            silver_df = self._deduplicate_records(silver_df, partition_cols)
            
            # Create Silver directory if it doesn't exist
            Path(silver_path).parent.mkdir(parents=True, exist_ok=True)
            
            # Write to Silver layer
            self.logger.info(f"Writing to Silver path: {silver_path}")
            (silver_df.write
                .format("parquet")
                .mode('append')
                .option("compression", "snappy")
                .save(silver_path))
            """
            (silver_df.write
             .format("delta")
             .mode("overwrite")
             .option("overwriteSchema", "true")
             .option("delta.enableChangeDataFeed", "true")
             .partitionBy("year", "month")
             .save(silver_path))
            """

            # Log transformation stats
            total_records = silver_df.count()
            valid_records = silver_df.filter(col("is_valid") == True).count()
            invalid_records = total_records - valid_records
            
            self.logger.info(f"Price transformation completed:")
            self.logger.info(f"  Total records: {total_records}")
            self.logger.info(f"  Valid records: {valid_records}")
            self.logger.info(f"  Invalid records: {invalid_records}")
            
        except Exception as e:
            self.logger.error(f"Error in price transformation: {str(e)}")
            raise

    def transform_installed_power_to_silver(self, country: str = "de") -> None:
        """Transform installed power data from Bronze to Silver"""
        
        self.logger.info("Starting installed power Bronze to Silver transformation")
        
        try:
            # Read from Bronze layer
            bronze_path = f"{self.config['delta_lake']['tables']['bronze_installed_power']}"
            silver_path = f"{self.config['delta_lake']['tables']['silver_installed_power']}"
            
            self.logger.info(f"Reading from Bronze path: {bronze_path}")
            bronze_df = self.spark.read.format("parquet").load(bronze_path)
            
            if bronze_df.count() == 0:
                self.logger.warning("No data found in Bronze installed_power table")
                return
            
            # Apply transformations - note 'time' column instead of 'timestamp'
            silver_df = bronze_df.select(
                col("timestamp").alias("timestamp"),
                col("production_type"),
                col("installed_capacity").alias("installed_power"),
                year(col("timestamp")).alias("year"),
                month(col("timestamp")).alias("month"),
                col("time_period"),
                col("country"),
                lit("GW").alias("unit"),
                col("source_system"),
                col("ingestion_date").alias("ingestion_timestamp"),
                current_timestamp().alias("processing_timestamp")
            )
            
            # Standardize production types
            silver_df = self._standardize_production_types(silver_df)
            
            # Apply data quality checks
            silver_df = self._apply_data_quality_checks(silver_df, "installed_power")
            
            # Remove duplicates
            partition_cols = ["timestamp", "production_type", "country"]
            silver_df = self._deduplicate_records(silver_df, partition_cols)
            
            # Create Silver directory if it doesn't exist
            Path(silver_path).parent.mkdir(parents=True, exist_ok=True)
            
            # Write to Silver layer
            self.logger.info(f"Writing to Silver path: {silver_path}")
            
            (silver_df.write
                .format("parquet")
                .mode('append')
                .option("compression", "snappy")
                .save(silver_path))

            """
            (silver_df.write
             .format("delta")
             .mode("overwrite")
             .option("overwriteSchema", "true")
             .option("delta.enableChangeDataFeed", "true")
             .partitionBy("year", "production_type")
             .save(silver_path)) """
            
            # Log transformation stats
            total_records = silver_df.count()
            valid_records = silver_df.filter(col("is_valid") == True).count()
            invalid_records = total_records - valid_records
            
            self.logger.info(f"Installed Power transformation completed:")
            self.logger.info(f"  Total records: {total_records}")
            self.logger.info(f"  Valid records: {valid_records}")
            self.logger.info(f"  Invalid records: {invalid_records}")
            
        except Exception as e:
            self.logger.error(f"Error in installed power transformation: {str(e)}")
            raise

    def transform_installed_power_sql(self) -> bool:
        """
        Transform bronze_installed_power to silver_installed_power using SQL transformation
        """
        try:
            self.logger.info("Starting Bronze to Silver transformation for installed power using SQL")
            
            # Read SQL transformation file
            sql_file_path = os.path.join(
                os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
                "pipelines", "sql", "bronze_to_silver_installed_power_transformation.sql"
            )
            
            if not os.path.exists(sql_file_path):
                self.logger.error(f"SQL transformation file not found: {sql_file_path}")
                return False
                
            with open(sql_file_path, 'r') as f:
                sql_content = f.read()
            
            # Create silver_installed_power table if it doesn't exist
            self._create_silver_installed_power_table()
            
            # Load bronze data as temporary view
            bronze_path = os.path.join(self.config['delta_lake']['storage']['bronze_path'], 
                                     'de', 'installed_power')
            
            if not os.path.exists(bronze_path):
                self.logger.warning(f"Bronze installed power data not found at: {bronze_path}")
                return False
                
            self.logger.info(f"Reading bronze data from: {bronze_path}")
            bronze_df = self.spark.read.parquet(bronze_path)
            bronze_df.createOrReplaceTempView("bronze_installed_power")
            
            self.logger.info(f"Bronze data loaded: {bronze_df.count()} records")
            
            # Split SQL into individual statements and execute
            sql_statements = [stmt.strip() for stmt in sql_content.split(';') if stmt.strip() and not stmt.strip().startswith('--')]
            
            results = []
            transformation_view_created = False
            
            for i, statement in enumerate(sql_statements):
                if statement.strip():
                    try:
                        self.logger.info(f"Executing SQL statement {i+1}/{len(sql_statements)}")
                        
                        # Skip empty statements or comments
                        if not statement.strip() or statement.strip().startswith('--'):
                            continue
                            
                        # Handle SET statements differently
                        if statement.strip().upper().startswith('SET'):
                            self.spark.sql(statement)
                            continue
                            
                        # Execute the statement
                        result_df = self.spark.sql(statement)
                        
                        # If it's a SELECT statement, collect and show results
                        if statement.strip().upper().startswith('SELECT'):
                            result = result_df.collect()
                            if result:
                                results.append(result)
                                self.logger.info(f"Statement {i+1} returned {len(result)} rows")
                                # Show first few rows for debugging
                                for row in result[:3]:
                                    self.logger.info(f"Sample result: {row}")
                        # Check if this creates the final transformation view
                        elif 'transformed_silver_installed_power' in statement:
                            transformation_view_created = True
                            self.logger.info(f"Transformation view created successfully")
                        else:
                            self.logger.info(f"Statement {i+1} executed successfully")
                            
                    except Exception as e:
                        self.logger.error(f"Error executing SQL statement {i+1}: {str(e)}")
                        self.logger.error(f"Statement: {statement[:200]}...")
                        # Don't fail the entire process for individual statement errors
                        continue
            
            # Save the transformed data to Parquet
            if transformation_view_created:
                try:
                    self.logger.info("Saving transformed silver data to Parquet")
                    silver_df = self.spark.sql("SELECT * FROM transformed_silver_installed_power")
                    silver_path = os.path.join(self.silver_path, 'energy_charts', 'de', 'installed_power')
                    
                    # Remove existing silver data
                    import shutil
                    if os.path.exists(silver_path):
                        shutil.rmtree(silver_path)
                        
                    os.makedirs(silver_path, exist_ok=True)
                    
                    # Write to Parquet
                    silver_df.coalesce(1).write.mode('overwrite').parquet(silver_path)
                    
                    record_count = silver_df.count()
                    self.logger.info(f"Silver installed power transformation completed successfully")
                    self.logger.info(f"Silver table created with {record_count} records at: {silver_path}")
                    
                    # Show sample of transformed data
                    self.logger.info("Sample silver data:")
                    silver_df.show(5, truncate=False)
                    
                    return True
                    
                except Exception as e:
                    self.logger.error(f"Error saving silver data: {str(e)}")
                    return False
            else:
                self.logger.error("Transformation view was not created")
                return False
                
        except Exception as e:
            self.logger.error(f"Error in SQL transformation for installed power: {str(e)}")
            return False
    
    def _create_silver_installed_power_table(self):
        """Create silver_installed_power table with proper schema"""
        try:
            # Create table directory if it doesn't exist
            silver_path = os.path.join(self.silver_path, 'energy_charts', 'de', 'installed_power')
            os.makedirs(silver_path, exist_ok=True)
            
            # Create empty DataFrame with proper schema
            from pyspark.sql.types import StructType, StructField, StringType, DoubleType, TimestampType, IntegerType, BooleanType
            
            schema = StructType([
                StructField("record_id", StringType(), False),
                StructField("business_key", StringType(), False),
                StructField("timestamp", TimestampType(), True),
                StructField("time_period", StringType(), False),
                StructField("production_type", StringType(), False),
                StructField("installed_capacity", DoubleType(), True),
                StructField("country", StringType(), False),
                StructField("data_source", StringType(), True),
                StructField("source_system", StringType(), True),
                StructField("dq_check_status", StringType(), True),
                StructField("dq_check_date", TimestampType(), True),
                StructField("dq_score", IntegerType(), True),
                StructField("is_anomaly", BooleanType(), True),
                StructField("bronze_record_id", StringType(), True),
                StructField("silver_batch_id", StringType(), True),
                StructField("silver_ingestion_date", TimestampType(), True),
                StructField("created_by", StringType(), True),
                StructField("last_updated", TimestampType(), True)
            ])
            
            # Create empty DataFrame and register as temp view
            empty_df = self.spark.createDataFrame([], schema)
            empty_df.createOrReplaceTempView("silver_installed_power")
            
            self.logger.info("Silver installed power table structure created")
            
        except Exception as e:
            self.logger.error(f"Error creating silver installed power table: {str(e)}")

    def run_all_transformations(self, country: str = "de") -> None:
        """Run all Bronze to Silver transformations"""
        
        self.logger.info("Starting all Bronze to Silver transformations")
        
        try:
            # Initialize schemas
            self._create_silver_schemas()
            
            # Transform each data type
            self.transform_public_power_to_silver(country)
            self.transform_price_to_silver(country)
            self.transform_installed_power_to_silver(country)
            
            self.logger.info("All Bronze to Silver transformations completed successfully")
            
        except Exception as e:
            self.logger.error(f"Error in Bronze to Silver transformations: {str(e)}")
            raise
    

# Convenience functions for individual job execution
def run_public_power_bronze_to_silver(spark: SparkSession, logger: Logger, config: Dict[str, Any], country: str = "de"):
    """Run public power Bronze to Silver transformation"""
    transformer = BronzeToSilverTransformation(spark, logger, config)
    transformer.transform_public_power_to_silver(country)

def run_price_bronze_to_silver(spark: SparkSession, logger: Logger, config: Dict[str, Any], country: str = "de"):
    """Run price Bronze to Silver transformation"""
    transformer = BronzeToSilverTransformation(spark, logger, config)
    transformer.transform_price_to_silver(country)

def run_installed_power_bronze_to_silver(spark: SparkSession, logger: Logger, config: Dict[str, Any], country: str = "de"):
    """Run installed power Bronze to Silver transformation using SQL"""
    transformer = BronzeToSilverTransformation(spark, logger, config)
    transformer.transform_installed_power_to_silver(country)
    success = None
    #success = transformer.transform_installed_power_sql()
    if success:
        logger.info("Installed power SQL transformation completed successfully")
    else:
        logger.error("Installed power SQL transformation failed")
        # Fallback to original method if SQL fails
        logger.info("Attempting fallback to original transformation method")
        transformer.transform_installed_power_to_silver(country)

def run_all_bronze_to_silver(spark: SparkSession, logger: Logger, config: Dict[str, Any], country: str = "de"):
    """Run all Bronze to Silver transformations"""
    transformer = BronzeToSilverTransformation(spark, logger, config)
    transformer.run_all_transformations(country)