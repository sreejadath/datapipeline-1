
# approach
The pipeline implements a medallion architecture with three distinct layers:
- **Bronze (Raw):** Exact copy of source data with minimal transformation
- **Silver (Refined):** Cleaned, deduplicated, and validated data
- **Gold (Curated):** Business-ready datasets optimized for analytics and ML

# assumptions and decision made
1. The data won't be changing time time for historical loads
2. Surrogate Keys UUID() for the id columns ensures uniqueness across environments and avoids dependency on source-system keys.
3. Delta Merge Operations:Optimized for deduplication and incremental updates.
4. Local Testing: Limit data volume with ingestion parameters and proceesing maximum 1 day data ata time.
5. Scalability:Partition Gold tables by timestamp for query optimization.



# set up and test the solution

# clone the repository from git

# install docker 

# Run the pipeline


approach, any assumptions and decision made, and how to set up and test the solution


# datapipeline
Repository for data pipeline code 

1. create and activate virtual environment

python3.11 -m venv delta_env

If not working try 
brew install python@3.11

source delta_env/bin/activate  

2. install packages 
pip install pyspark==3.5.0 delta-spark==3.0.0 requests==2.31.0 great-expectations==0.18.2
pip install -r requirements.txt

export PYSPARK_PYTHON=python3.11       # Worker nodes
export PYSPARK_DRIVER_PYTHON=python3.11  # Driver node


3. Run the demo project

python src/main.py --job demo --config config/pipeline_config.yaml

spark-submit     --packages io.delta:delta-core_2.12:3.1.0   --conf "spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension"   --conf "spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog"    src/main.py --job demo --layer silver --config config/pipeline_config.yaml

verify environments 

java --version

openjdk 11.0.27 2025-04-15
OpenJDK Runtime Environment Temurin-11.0.27+6 (build 11.0.27+6)
OpenJDK 64-Bit Server VM Temurin-11.0.27+6 (build 11.0.27+6, mixed mode)

