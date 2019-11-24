#!/usr/bin/env bash

trap "kill 0" EXIT

#PIDS=()

ECS_HOST="localhost"
ECS_PORT=5050
ECS_ADDR="${ECS_HOST}:${ECS_PORT}"

CACHE_DISPLACEMENT="LRU"
CACHE_SIZE=100
START_PORT=5150
DATA_DIR="data"
LOG_LEVEL="INFO"
LOG_FILE="kvserver.log"

java -jar target/ecs-server.jar -p ${ECS_PORT} -ll=${LOG_LEVEL} &

for i in $(seq 10)
do
  java -jar target/kv-server.jar \
    -c ${CACHE_SIZE} \
    -s=${CACHE_DISPLACEMENT} \
    -b ${ECS_ADDR} \
    -p $((START_PORT + i)) \
    -d ${DATA_DIR}/kv"${i}" \
    -l "${i}.${LOG_FILE}" \
    -ll=${LOG_LEVEL} &

#    PIDS+=($!)
done

#while true;
#do
#  printf "Open files: \n";
#  for pid in "${PIDS[@]}"; do lsof -p "${pid}" | wc -l; done
#  sleep 2;
#done

wait