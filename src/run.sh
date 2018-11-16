#!/usr/bin/sh
export PYTHONIOENCODING=utf8
export RUN_ON_CLUSTER=1
spark-submit --master yarn --deploy-mode client --driver-memory 8G --num-executors 10 --executor-memory 6G --executor-cores 5 "$@"