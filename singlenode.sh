#!/bin/bash

set -x

export JARPATH=$(realpath $(dirname $0))

export CUDA_VERSION=cuda11
export CUDF_VERSION=0.19.2
export R4S_VERSION=0.5.0

test -f Pipenv.lock || pipenv install

test -f cudf-${CUDF_VERSION}-${CUDA_VERSION}.jar || curl https://repo1.maven.org/maven2/ai/rapids/cudf/${CUDF_VERSION}/cudf-${CUDF_VERSION}-${CUDA_VERSION}.jar -o cudf-${CUDF_VERSION}-${CUDA_VERSION}.jar 
test -f rapids-4-spark_2.12-${R4S_VERSION}.jar || curl https://repo1.maven.org/maven2/com/nvidia/rapids-4-spark_2.12/${R4S_VERSION}/rapids-4-spark_2.12-${R4S_VERSION}.jar -o rapids-4-spark_2.12-${R4S_VERSION}.jar

if [[ -v LIGHTRUN ]]; then
    DUP_TIMES="--dup-times 50"
else
    DUP_TIMES="--dup-times 5000"
fi

mkdir tmp
export SPARK_TEMP=$(pwd)/tmp

test -d billing_events.parquet || pipenv run spark-submit --driver-memory 64G --master="local[*]" --conf spark.eventLog.enabled=True --conf spark.eventLog.dir=${SPARK_TEMP} --conf spark.default.parallelism=32 --conf spark.sql.analyzer.failAmbiguousSelfJoin=false --conf spark.local.dir=${SPARK_TEMP} ./generate.py ${DUP_TIMES} 
pipenv run spark-submit --driver-memory 32G --master="local[*]" --jars ${JARPATH}/cudf-*.jar,${JARPATH}/rapids-4-spark_2.12-*.jar --conf spark.eventLog.enabled=True --conf spark.eventLog.dir=${SPARK_TEMP} --conf spark.plugins=com.nvidia.spark.SQLPlugin --conf spark.sql.adaptive.enabled=true --conf spark.rapids.sql.batchSizeBytes=1073741824 --conf spark.rapids.sql.concurrentGpuTasks=2 --conf spark.default.parallelism=32 --conf spark.rapids.sql.explain=NOT_ON_GPU --conf spark.rapids.sql.decimalType.enabled=true --conf spark.rapids.sql.variableFloatAgg.enabled=true --conf spark.local.dir=${SPARK_TEMP} ./do-analytics.py --log-level WARN
