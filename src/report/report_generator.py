from pyspark.sql import SparkSession, DataFrame
from src.logger.custom_logger import Logger
from typing import Dict, Any
import pandas as pd
from delta.tables import DeltaTable
from datetime import datetime
import os

class DeltaLakeReporter:
    def __init__(self, spark: SparkSession, logger: Logger, config: Dict[str, Any]):
        self.spark = spark
        self.logger = logger
        self.config = config
        self.gold_paths = config['delta_lake']['tables']
        self.report_dir = 'reports'
        os.makedirs(self.report_dir, exist_ok=True)
    
    def _read_delta_table(self, path):
        """Helper function to read Delta table as pandas DataFrame"""
        delta_table = self.spark.read.parquet(path)
        return delta_table.toPandas()
    
    def _save_html_report(self, df, report_name):
        """Save DataFrame as HTML report with timestamp"""
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"{report_name}_{timestamp}.html"
        filepath = os.path.join(self.report_dir, filename)
        
        # Create HTML with some basic styling
        html = f"""
        <html>
            <head>
                <title>{report_name}</title>
                <style>
                    body {{ font-family: Arial, sans-serif; margin: 20px; }}
                    h1 {{ color: #2c3e50; }}
                    table {{ border-collapse: collapse; width: 100%; }}
                    th, td {{ border: 1px solid #ddd; padding: 8px; text-align: left; }}
                    th {{ background-color: #f2f2f2; }}
                    tr:nth-child(even) {{ background-color: #f9f9f9; }}
                </style>
            </head>
            <body>
                <h1>{report_name}</h1>
                <p>Generated on: {datetime.now().strftime("%Y-%m-%d %H:%M:%S")}</p>
                {df.to_html(index=False)}
            </body>
        </html>
        """
        
        with open(filepath, 'w') as f:
            f.write(html)
        print(f"Report saved to: {filepath}")
    
    def generate_daily_production_trend_report(self):
        """Generate report on daily electricity production trends in Germany"""
        print("Generating Daily Production Trend Report...")
        
        # Read necessary tables
        fact_power = self._read_delta_table(self.gold_paths['gold_fact_power'])
        dim_production_type = self._read_delta_table(self.gold_paths['gold_dim_production_type'])
        
        # Execute query
        query_result = fact_power.merge(
            dim_production_type,
            on='production_type_id',
            how='inner'
        )
        
        query_result = query_result[query_result['country_x'] == 'de']
        
        report_df = query_result.groupby(
            ['year', 'month', 'day', 'production_plant_name']
        )['electricity_produced'].sum().reset_index()
        
        report_df.columns = ['year', 'month', 'day', 'production_type', 'total_daily_production']
        report_df = report_df.sort_values(['year', 'month', 'day', 'production_type'])
        
        # Save report
        self._save_html_report(report_df, "daily_production_trend")
    
    def generate_underperformance_prediction_report(self):
        """Generate report for underperformance prediction"""
        print("Generating Underperformance Prediction Report...")
        
        # Read necessary tables
        fact_power_30min = self._read_delta_table(self.gold_paths['gold_fact_power_30min_agg'])
        dim_production_type = self._read_delta_table(self.gold_paths['gold_dim_production_type'])
        
        # Execute query
        merged_df = fact_power_30min.merge(
            dim_production_type,
            on='production_type_id',
            how='inner'
        )
        
        merged_df = merged_df[
            (merged_df['country_x'] == 'de') & 
            (merged_df['active_flag'] == True)
        ]
        
        # Calculate lag features and rolling averages
        report_df = merged_df.sort_values(['production_type_id', 'timestamp_30min'])
        
        # Group by production type to calculate lags
        grouped = report_df.groupby('production_type_id')
        
        # Calculate lag features
        report_df['lag_1d'] = grouped['total_electricity_produced'].shift(48)
        report_df['lag_1w'] = grouped['total_electricity_produced'].shift(336)
        
        # Calculate rolling 7-day average (same time of day)
        report_df['rolling_7d_avg'] = grouped.apply(
            lambda x: x['total_electricity_produced'].rolling(336, min_periods=1).mean()
        ).reset_index(level=0, drop=True)
        
        # Select final columns
        report_df = report_df[[
            'timestamp_30min', 'production_type_id', 'production_plant_name',
            'energy_category', 'controllability_type', 'total_electricity_produced',
            'year', 'month', 'day', 'hour', 'minute_interval_30',
            'lag_1d', 'lag_1w', 'rolling_7d_avg'
        ]]
        
        # Save report
        self._save_html_report(report_df, "underperformance_prediction")
    
    def generate_wind_price_analysis_report(self):
        """Generate report on wind power price analysis"""
        print("Generating Wind Price Analysis Report...")
        
        # Read necessary tables
        fact_power = self._read_delta_table(self.gold_paths['gold_fact_power'])
        dim_production_type = self._read_delta_table(self.gold_paths['gold_dim_production_type'])
        
        # Execute query
        merged_df = fact_power.merge(
            dim_production_type,
            on='production_type_id',
            how='inner'
        )
        
        merged_df = merged_df[
            (merged_df['country_x'] == 'de') & 
            (merged_df['production_plant_name'].isin(['Wind_Offshore', 'Wind_Onshore'])) &
            (merged_df['active_flag'] == True)
        ]
        
        report_df = merged_df.groupby(
            ['year', 'month', 'day', 'production_plant_name']
        ).agg({
            'electricity_produced': 'sum',
            'electricity_price': 'mean'
        }).reset_index()
        
        report_df.columns = ['year', 'month', 'day', 'production_type', 
                           'total_daily_production_mw', 'avg_daily_price_eur_per_mwh']
        
        report_df = report_df.sort_values(['year', 'month', 'day', 'production_type'])
        
        # Save report
        self._save_html_report(report_df, "wind_price_analysis")
    
    def generate_all_reports(self):
        """Generate all defined reports"""
        self.generate_daily_production_trend_report()
        self.generate_underperformance_prediction_report()
        self.generate_wind_price_analysis_report()
        print("All reports generated successfully!")

# Convenience functions for individual job execution
def run_reports(spark: SparkSession, logger: Logger, config: Dict[str, Any], country: str = "de"):
    reporter = DeltaLakeReporter(spark, logger, config)
    reporter.generate_all_reports()