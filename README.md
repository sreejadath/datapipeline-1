# datapipeline
Repository for data pipeline code 

1. create and activate virtual environment

python3.11 -m venv delta_env

If not working try 
brew install python@3.11

source delta_env/bin/activate  

2. install packages 
pip install pyspark==3.5.0 delta-spark==3.1.0
pip install -r requirements.txt

export PYSPARK_PYTHON=python3.11       # Worker nodes
export PYSPARK_DRIVER_PYTHON=python3.11  # Driver node


3. Run the demo project

python src/main.py --job demo --layer silver --config config/pipeline_config.yaml

spark-submit     --packages io.delta:delta-core_2.12:3.1.0   --conf "spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension"   --conf "spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog"    src/main.py --job demo --layer silver --config config/pipeline_config.yaml

verify environments 

java --version

openjdk 11.0.27 2025-04-15
OpenJDK Runtime Environment Temurin-11.0.27+6 (build 11.0.27+6)
OpenJDK 64-Bit Server VM Temurin-11.0.27+6 (build 11.0.27+6, mixed mode)

