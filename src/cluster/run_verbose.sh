#!/usr/bin/sh
export PYTHONIOENCODING=utf8
export RUN_ON_CLUSTER=1

zip dependencies config.py load_datasets.py username.py

spark-submit \
--master yarn --deploy-mode cluster --driver-memory 32G --num-executors 10 --executor-memory 12G --executor-cores 5 --py-files dependencies.zip "$@" 2>&1