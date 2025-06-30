import sys
import os

# Add the project root to Python path
project_root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.insert(0, project_root)

from datetime import datetime, timedelta
from src.pipelines.data_ingestion import DataIngestionJob
import pytz

def ingest_installed_power_job(spark, logger, connector, config, bulk_load_flag:bool , **kwargs):
    """
    Ingest installed power data (monthly capacity data).
    """
    logger.info(f"Installed Power Job:..................................................")

    # Get current UTC time
    now = datetime.now(pytz.utc)
    country = kwargs.get('country', config['energy_charts_api']['default_params']['country'])
    endpoint = kwargs.get('endpoint', 'installed_power')
    time_step = kwargs.get('time_step', 'monthly')

    ingestion_job = DataIngestionJob(spark, logger, config, endpoint, now.date().isoformat(), country)
    logger.info(f"Job: ingestion_job created for {endpoint} with country {country}")

    try:
        logger.info(f"Job: Ingesting installed power data for country {country} with time_step {time_step}")
        
        api_data = connector.get_api_data(
            endpoint=endpoint,
            start_date='',
            end_date='',
            time_step=time_step,
            country=country
        )

        if api_data:
            logger.info(f"Job: Data found for {time_step}, proceeding with ingestion")
            ingestion_job.run(api_data)
            logger.info(f"Job: Successfully ingested installed power data")
        else:
            logger.warning(f"Job: No data found for {endpoint} and country {country}")
            
    except Exception as e:
        logger.error(f"Job: Error ingesting data for {endpoint} and country {country}: {str(e)}")
        raise e