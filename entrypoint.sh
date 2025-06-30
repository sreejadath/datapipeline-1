#!/bin/bash

# Start Jupyter Lab in background if JUPYTER_MODE is set
if [ "$JUPYTER_MODE" = "true" ]; then
    jupyter lab \
        --ip=0.0.0.0 \
        --port=${JUPYTER_PORT} \
        --no-browser \
        --allow-root \
        --notebook-dir=/app/notebooks &
fi

# Run the main pipeline
#exec python src/main.py --job installed_power --config config/pipeline_config.yaml
#exec python src/main.py --job public_power --config config/pipeline_config.yaml
#exec python src/main.py --job price --config config/pipeline_config.yaml
# Wait for all background processes to finish
wait  