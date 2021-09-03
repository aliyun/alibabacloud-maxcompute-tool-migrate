#
# Copyright 1999-2021 Alibaba Group Holding Ltd.
# 
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# 
#      http://www.apache.org/licenses/LICENSE-2.0
# 
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
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
