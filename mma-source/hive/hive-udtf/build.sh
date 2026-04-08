#!/bin/bash

function exit_on_fail() {
    exit_code=$?

    if [ ${exit_code} != 0 ]; then
      echo $1
      exit ${exit_code}
    fi
}

hive_versions=('1' '3' '2')

for hv in "${hive_versions[@]}"; do
  echo "build hive${hv}"
  mvn clean package -Dhive=${hv}
  exit_on_fail "failed build hive${hv}"

  cp target/hive-mma-udtf.jar ../../../mma-server/src/main/resources/mma-udtf-hive${hv}.jar
done