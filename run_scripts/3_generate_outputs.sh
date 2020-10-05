#!/usr/bin/env bash

set -e

while [[ $# -gt 0 ]]; do
    case "$1" in
        --profile-cpu)
            CPU_PROFILE_OUTPUT_PATH="$2"
            CPU_PROFILE_ARG="--profile-cpu $CPU_PROFILE_OUTPUT_PATH"
            shift 2;;
        --profile-memory)
            MEMORY_PROFILE_OUTPUT_PATH="$2"
            MEMORY_PROFILE_ARG="--profile-memory $MEMORY_PROFILE_OUTPUT_PATH"
            shift 2;;
        --)
            shift
            break;;
        *)
            break;;
    esac
done

if [[ $# -ne 4 ]]; then
    echo "Usage: ./3_generate_outputs.sh [--profile-cpu <cpu-profile-output-path>] [--profile-memory <memory-profile-output-path>] <user> <pipeline-run-mode>\
          <pipeline-configuration-file-path> <data-root>"
    echo "Generates ICR files, Coda files, production CSV and analysis CSVs from the raw data files produced by run scripts 1 and 2"
    exit
fi

USER=$1
PIPELINE_RUN_MODE=$2
PIPELINE_CONFIGURATION_FILE_PATH=$3
DATA_ROOT=$4

if [ -d "$DATA_ROOT/Outputs" ]
then
    rm -r "$DATA_ROOT/Outputs"
fi

mkdir -p "$DATA_ROOT/Outputs"

cd ..
./docker-run-generate-outputs.sh ${CPU_PROFILE_ARG} ${MEMORY_PROFILE_ARG} \
    "$USER" "$PIPELINE_RUN_MODE" "$PIPELINE_CONFIGURATION_FILE_PATH" \
    "$DATA_ROOT/Raw Data" "$DATA_ROOT/Coded Coda Files/" "$DATA_ROOT/Outputs/auto_coding_traced_data.jsonl" \
    "$DATA_ROOT/Outputs/messages_traced_data.jsonl" "$DATA_ROOT/Outputs/individuals_traced_data.jsonl" \
    "$DATA_ROOT/Outputs/ICR/" "$DATA_ROOT/Outputs/Coda Files/" \
    "$DATA_ROOT/Outputs/messages.csv" "$DATA_ROOT/Outputs/individuals.csv" \
    "$DATA_ROOT/Outputs/production.csv"
