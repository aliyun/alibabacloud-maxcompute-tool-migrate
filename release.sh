#!/bin/bash
set -e

echo "Release starts"

echo "Build release branch origin/release/hive-1.x"
git checkout --quiet release/hive-1.x
sh build.sh
mv mma.tar.gz mma-hive-1.x-release.tar.gz
mv mma.tar.gz.md5sum mma-hive-1.x-release.tar.gz.md5sum
echo "Done"

echo "Build release branch origin/release/hive-2.x"
git checkout --quiet release/hive-2.x
sh build.sh
mv mma.tar.gz mma-hive-2.x-release.tar.gz
mv mma.tar.gz.md5sum mma-hive-2.x-release.tar.gz.md5sum
echo "Done"
