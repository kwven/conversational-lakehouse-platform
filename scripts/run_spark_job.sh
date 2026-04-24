#!/usr/bin/env bash
set -euo pipefail

if [[ $# -lt 1 ]]; then
  echo "Usage: $0 <job-path-under-repo> [extra spark-submit args...]" >&2
  echo "Example: $0 spark/jobs/bronze/orders_cdc_to_delta.py" >&2
  exit 1
fi

job_path="$1"
shift

if [[ "$job_path" != /workspace/* ]]; then
  job_path="/workspace/${job_path#./}"
fi

runtime_root="/tmp/conversational-lakehouse-platform"
ivy_dir="$runtime_root/spark-ivy"

mkdir -p "$ivy_dir/cache" "$ivy_dir/jars"
chmod 1777 "$runtime_root" "$ivy_dir" "$ivy_dir/cache" "$ivy_dir/jars"

docker compose --profile manual run --rm --no-deps --use-aliases \
  --entrypoint /bin/bash \
  -v "$ivy_dir:/opt/spark/ivy" \
  -e PYTHONPATH=/opt/spark/python:/opt/spark/python/lib/py4j-0.10.9.7-src.zip:/workspace:/workspace/spark \
  -e SPARK_PROPERTIES_FILE \
  -e KAFKA_BOOTSTRAP_SERVERS \
  -e STREAM_STARTING_OFFSETS \
  -e STREAM_MAX_OFFSETS_PER_TRIGGER \
  -e CHECKPOINT_LOCATION \
  -e BRONZE_PATH \
  -e KAFKA_TOPIC_PREFIX \
  spark-submit \
  -lc '
    set -euo pipefail
    driver_ip="$(hostname -I 2>/dev/null | awk "{print \$1}")"
    ivy_dir="/opt/spark/ivy"
    properties_file="${SPARK_PROPERTIES_FILE:-/workspace/spark/conf/spark-submit-defaults.conf}"

    if [[ -z "${driver_ip}" ]]; then
      driver_ip="$(hostname -i | awk "{print \$1}")"
    fi

    if [[ -z "${driver_ip}" ]]; then
      echo "Unable to determine the spark-submit container IP address" >&2
      exit 1
    fi

    export SPARK_LOCAL_IP="${driver_ip}"
    mkdir -p "${ivy_dir}/cache" "${ivy_dir}/jars"
    export SPARK_SUBMIT_OPTS="${SPARK_SUBMIT_OPTS:-} -Divy.home=${ivy_dir} -Divy.cache.dir=${ivy_dir}/cache"

    exec /opt/spark/bin/spark-submit \
      --properties-file "${properties_file}" \
      --conf "spark.driver.host=${driver_ip}" \
      --conf "spark.driver.bindAddress=0.0.0.0" \
      --conf "spark.jars.ivy=${ivy_dir}" \
      "$@"
  ' spark-submit "$@" "$job_path"
