#!/usr/bin/sh

# Launch a python script on the cluster, usage: ./run_verbose.sh script.py
# You must create a file username.py with a global variable "user = gaspar_account"

export PYTHONIOENCODING=utf8
export RUN_ON_CLUSTER=1

# create the zip with the dependencies
zip dependencies config.py load_datasets.py username.py

spark-submit \
--master yarn --deploy-mode cluster --driver-memory 32G --num-executors 4 --executor-memory 8G --py-files dependencies.zip "$@" 2>&1