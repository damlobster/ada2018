#!/usr/bin/sh
export PYTHONIOENCODING=utf8
export RUN_ON_CLUSTER=1
spark-submit \
--master yarn --deploy-mode client --driver-memory 32G --num-executors 5 --executor-memory 32G --executor-cores 10 "$@" 2>&1 | grep -v " INFO "