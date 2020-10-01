#!/usr/bin/env bash

set -e

if [[ $# -ne 6 ]]; then
    echo "Usage: ./7_upload_analysis_files.sh <user> <pipeline-run-mode> <google-cloud-credentials-file-path> <pipeline-configuration-file-path> <run-id> <data-dir>"
    echo "Uploads the pipeline's analysis files"
    exit
fi

USER=$1
PIPELINE_RUN_MODE=$2
GOOGLE_CLOUD_CREDENTIALS_FILE_PATH=$3
PIPELINE_CONFIGURATION_FILE_PATH=$4
RUN_ID=$5
DATA_ROOT=$6

cd ..
./docker-run-upload-analysis-files.sh "$USER" "$PIPELINE_RUN_MODE" "$GOOGLE_CLOUD_CREDENTIALS_FILE_PATH" "$PIPELINE_CONFIGURATION_FILE_PATH" "$RUN_ID" \
    "$DATA_ROOT/Outputs/production.csv" "$DATA_ROOT/Outputs/messages.csv" "$DATA_ROOT/Outputs/individuals.csv" \
    "$DATA_ROOT/Outputs/Automated Analysis/"
