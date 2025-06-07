#!/bin/bash

CONTAINER_NAME="pg4kamino"
IMAGE_PATH="./postgres_11.sif"
SINGULARITY_CACHE="../.singularity"
PG_DATA_DIR="../postgres_data"
PG_RUN_DIR="../postgres_run"

singularity instance stop db
rm -rf $PG_DATA_DIR $PG_RUN_DIR
mkdir -p $PG_DATA_DIR $PG_RUN_DIR
export SINGULARITYENV_POSTGRES_DB="db4kamino"
export SINGULARITYENV_POSTGRES_USER="kamino"
export SINGULARITYENV_POSTGRES_PASSWORD="kamino"
singularity instance start -C --bind /tmp:/tmp --bind $PG_DATA_DIR:/var/lib/postgresql/data --bind $PG_RUN_DIR:/var/run/postgresql docker://postgres:11 db
singularity run instance://db > ./db_logs.log &