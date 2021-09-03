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
#!/bin/bash
set -e

echo "Release starts"

MMA_VERSION=$(mvn help:evaluate -Dexpression=project.version -q -DforceStdout)
echo "Version: ${MMA_VERSION}"

echo "Build release branch origin/release/hive-1.x"
git checkout --quiet release/hive-1.x
mvn -U -q clean package -DskipTests
mv distribution/target/mma-${MMA_VERSION}.zip mma-${MMA_VERSION}-hive-1.x.zip
echo "Done"

echo "Build release branch origin/release/hive-2.x"
git checkout --quiet release/hive-2.x
mvn -U -q clean package -DskipTests
mv distribution/target/mma-${MMA_VERSION}.zip mma-${MMA_VERSION}-hive-2.x.zip
echo "Done"

echo "Build release branch origin/release/hive-3.x"
git checkout --quiet release/hive-3.x
mvn -U -q clean package -DskipTests
mv distribution/target/mma-${MMA_VERSION}.zip mma-${MMA_VERSION}-hive-3.x.zip
echo "Done"