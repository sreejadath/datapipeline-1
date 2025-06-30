FROM bde2020/spark-python-template:3.3.0-hadoop3.3
	  
COPY wordcount.py /app/



ENV SPARK_APPLICATION_PYTHON_LOCATION /app/src/main.py
ENV SPARK_APPLICATION_ARGS "/README.md"
#ENV PYSPARK_SUBMIT_ARGS="--packages io.delta:delta-spark_2.12:${DELTA_VERSION} pyspark-shell"

ENV PYSPARK_VERSION=3.5.0 \
    DELTA_VERSION=3.0.0 \
    PYTHON_VERSION=3.10 \
    SPARK_HOME=/opt/spark \
    PATH=$PATH:/opt/spark/bin \
    JUPYTER_PORT=8888


RUN pip install jupyterlab ipykernel findspark


WORKDIR /app

COPY requirements.txt .
RUN python -m pip install --upgrade pip && \
    python -m pip install -r requirements.txt --verbose

# Copy the rest of the project files
COPY . .

# -----------------------------------------------------------------------------
# Ensure the Delta Lake data directory exists
# -----------------------------------------------------------------------------
RUN mkdir -p /app/delta_lake && \
    chmod 777 /app/delta_lake



# -----------------------------------------------------------------------------
# Expose Jupyter port
# -----------------------------------------------------------------------------
EXPOSE ${JUPYTER_PORT}

# -----------------------------------------------------------------------------
# Entrypoint and startup scripts
# -----------------------------------------------------------------------------
COPY entrypoint.sh /entrypoint.sh
RUN chmod +x /entrypoint.sh
ENTRYPOINT ["/entrypoint.sh"]



# -----------------------------------------------------------------------------
# Example: To run a specific pipeline job, override CMD or use docker run arguments
# -----------------------------------------------------------------------------
# CMD ["python", "src/main.py", "--job", "installed_power", "--config", "config/pipeline_config.yaml"]
