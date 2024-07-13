#!/usr/bin/env bash

process=$(pgrep -laf jupyter-lab)
if [ -z "$process" ]; then
    echo "Starting Jupyter Lab"
    nohup jupyter-lab --no-browser --port=8888 --ip=0.0.0.0 --allow-root --ServerApp.iopub_data_rate_limit=2000000.0 > /dev/null 2>&1 &
    jupyter-lab list
else
    echo "Jupyter Lab is already running"
    jupyter-lab list
fi