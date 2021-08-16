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