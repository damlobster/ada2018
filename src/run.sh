#!/usr/bin/sh
export PYTHONIOENCODING=utf8
export RUN_ON_CLUSTER=1
spark-submit \
--files $(pwd)'/log4j.properties' \
--conf 'spark.executor.extraJavaOptions=-Dlog4j.configuration='$(pwd)'/log4j.properties' \
--conf 'spark.driver.extraJavaOptions=-Dlog4j.configuration='$(pwd)'/log4j.properties' \
--master yarn --deploy-mode client --driver-memory 32G --num-executors 5 --executor-memory 32G --executor-cores 10 "$@"