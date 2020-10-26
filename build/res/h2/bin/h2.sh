#!/bin/sh
CUR_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"
MMA_HOME=$(dirname "${CUR_DIR}/../../../..")

if [ -f "${MMA_HOME}/.MmaMeta.mv.db" ]
then
  java -cp "${CUR_DIR}/h2-1.4.200.jar" org.h2.tools.Shell \
  -url "jdbc:h2:file:${MMA_HOME}/.MmaMeta;AUTO_SERVER=TRUE" \
  -user mma \
  -password mma
else
  echo "MMA metadata not found in ${MMA_HOME}"
fi
