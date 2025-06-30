import sys
import os

# Add the project root to Python path
project_root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.insert(0, project_root)

from datetime import datetime, timedelta
from src.pipelines.data_ingestion import DataIngestionJob
import pytz

def ingest_public_power_job(spark, logger, job_id, connector, config, bulk_load_flag:bool , **kwargs):
    """
    Ingest api data for a given date range.
    """
    logger.info(f"Public power Job:..................................................")
    if bulk_load_flag:
        
        start_date = kwargs.get('start_date', config['energy_charts_api']['default_params']['start_date'])
        end_date = kwargs.get('end_date', config['energy_charts_api']['default_params']['end_date'])
        country = kwargs.get('country', config['energy_charts_api']['default_params']['country'])
        endpoint = kwargs.get('endpoint', 'public_power')

        logger.info("Job: Bulk load flag is set to True, ingesting all data in one go")
        current_date = datetime.strptime(start_date, "%Y-%m-%d").date()
        end_date_obj = datetime.strptime(end_date, "%Y-%m-%d").date()

        ingestion_job = DataIngestionJob(spark, logger, job_id, config, endpoint, current_date, country)
        logger.info(f"Job: ingestion_job created for {current_date}")

        while current_date <= end_date_obj:
            logger.info(f"Job: Ingesting for date: {current_date}")

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
        endpoint = kwargs.get('endpoint', 'public_power')
        
        # Get current UTC time
        now = datetime.now(pytz.utc)

        # Round down to the last 15-minute mark
        rounded_minute = (now.minute // interval) * interval
        start_time = now.replace(minute=rounded_minute, second=0, microsecond=0)

        # The interval we're interested in is the **previous** 15-minute block
        end_time = start_time
        start_time = end_time - timedelta(minutes=interval)
        logger.info(f"Job: Ingesting data for the last {interval} minutes from {start_time} to {end_time}")
        try:
            api_data = connector.get_api_data(
                endpoint=endpoint,
                start_date=start_time.isoformat(),  
                end_date=end_time.isoformat(),    
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
        