#!/bin/bash

set -x

export JARPATH=$(realpath $(dirname $0))

if [ x$"LIGHTRUN" != "x" ]; then
    DUP_TIMES="--dup-times 10000"
else
    DUP_TIMES="--dup-times 100"
fi

pipenv run spark-submit --driver-memory 16G --master="local[*]" ./generate.py ${DUP_TIMES}
pipenv run spark-submit --driver-memory 16G --master="local[*]" --jars ${JARPATH}/cudf-*.jar,${JARPATH}/rapids-4-spark_2.12-*.jar --conf spark.plugins=com.nvidia.spark.SQLPlugin --conf spark.sql.adaptive.enabled=true --conf spark.rapids.sql.batchSizeBytes=1073741824 --conf spark.rapids.sql.concurrentGpuTasks=4 --conf spark.default.parallelism=256 ./do-etl.py
