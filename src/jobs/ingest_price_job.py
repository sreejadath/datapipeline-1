import sys
import os

# Add the project root to Python path
project_root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.insert(0, project_root)

from datetime import datetime, timedelta
from src.pipelines.data_ingestion import DataIngestionJob
import pytz

def ingest_price_job(spark, logger, job_id, connector, config, bulk_load_flag:bool , **kwargs):
    """
    Ingest api data for a given date range.
    """
    logger.info(f"Price Job:..................................................")

     # Get current UTC time
    now = datetime.now(pytz.utc)
    if bulk_load_flag:
        logger.info("Job: Bulk load flag is set to True, ingesting all data in one go")
        # Loop daily within the given range for ingestion (example: daily ingestion)
        start_date = kwargs.get('start_date', config['energy_charts_api']['default_params']['start_date'])
        end_date = kwargs.get('end_date', config['energy_charts_api']['default_params']['end_date'])
        country = kwargs.get('country', config['energy_charts_api']['default_params']['country'])
        endpoint = kwargs.get('endpoint', 'price')

        current_date = datetime.strptime(start_date, "%Y-%m-%d").date()
        end_date_obj = datetime.strptime(end_date, "%Y-%m-%d").date()


        ingestion_job = DataIngestionJob(spark, logger, job_id, config, endpoint, current_date, country)
        logger.info(f"Job: ingestion_job created for {current_date}")

        if end_date_obj >= now.date():
            privious_day = now.date() - timedelta(days=1)
            logger.warning(f"Job: End date {end_date_obj} is in the future. Adjusting to previous day {privious_day}.")
            end_date_obj = privious_day

        while current_date <= end_date_obj:
            logger.info(f"Job: Ingesting for endpoint:{endpoint} date: {current_date}")

            try:
                api_data = connector.get_api_data(
                    endpoint=endpoint,
                    start_date=str(current_date),
                    end_date=str(current_date),
                    country=country
                )

                if api_data:
                    logger.info(f"Job: Data found for {current_date}, proceeding with ingestion")
                    ingestion_job.run(api_data)
                    logger.info(f"Job: Successfully ingested data for {current_date}")
                else:
                    logger.warning(f"Job: No data found for {current_date}")
                    
            except Exception as e:
                logger.error(f"Job: Error ingesting data for {current_date}: {str(e)}")

            current_date += timedelta(days=1)


    else:
        interval = kwargs.get('interval', config['energy_charts_api']['schedule']['public_power_interval_minutes'])
        country = kwargs.get('country', config['energy_charts_api']['default_params']['country'])
        endpoint = kwargs.get('endpoint', 'price')

        privious_day = now.date() - timedelta(days=1)
        logger.info(f"Job: Ingesting data for the last {interval} day from {privious_day}")
        try:
            api_data = connector.get_api_data(
                endpoint=endpoint,
                start_date=privious_day.isoformat(),  
                end_date=privious_day.isoformat(),    
                country=country
            )

            if api_data:
                logger.info(f"Job: Data found for the last {interval} minutes, proceeding with ingestion")
                ingestion_job.run(api_data)
                logger.info("Job: Successfully ingested data for the last interval")
            else:
                logger.warning("Job: No data found for the last interval")
        except Exception as e:
            logger.error(f"Job: Error ingesting data for the last interval: {str(e)}")
        