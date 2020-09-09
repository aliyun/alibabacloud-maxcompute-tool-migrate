#!/bin/bash
set -e

echo "Release starts"

echo "Build release branch release/hive-1.x"
git checkout --quiet release/hive-1.x
sh build.sh
mv mma.tar.gz mma-hive-1.x-release.tar.gz
echo "Done"

echo "Build release branch release/hive-2.x"
git checkout --quiet release/hive-2.x
sh build.sh
mv mma.tar.gz mma-hive-2.x-release.tar.gz
echo "Done"