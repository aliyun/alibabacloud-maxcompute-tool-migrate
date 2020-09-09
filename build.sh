#!/bin/bash
set -e

echo "Build starts"

if [ -d mma ]
then
  echo "Directory mma exists, remove it"
  rm -rf mma
  echo "Done"
fi

echo "Create build directory mma"
mkdir mma
echo "Done"

echo "Install local jars"
mvn --quiet install:install-file \
-Dfile=task-scheduler/resource/taobao-sdk-java-auto_1479188381469-20200701.jar \
-DgroupId=com.dingtalk \
-DartifactId=dingtalk-sdk \
-Dversion=1.0 \
-Dpackaging=jar
echo "Done"

echo "Compile"
mvn --quiet clean package -DskipTests
echo "Done"

echo "Assemble"
echo "  Copy executable files"
cp -r build/bin mma/bin
echo "  Done"
echo "  Copy jars"
mkdir mma/lib
cp task-scheduler/target/task-scheduler-1.0-SNAPSHOT.jar mma/lib/
cp data-transfer-hive-udtf/target/data-transfer-hive-udtf-1.0-SNAPSHOT-jar-with-dependencies.jar mma/lib
echo "  Done"
echo "  Copy configuration files"
cp  -r build/conf/ mma/conf
echo "  Done"
echo "  Copy resources"
cp -r build/res mma/res
echo "  Done"
echo "  Package"
tar cpfz mma.tar.gz mma
echo "  Done"
echo "Done"

echo "Clean up"
rm -rf mma
echo "Done"

echo "Build finished"
