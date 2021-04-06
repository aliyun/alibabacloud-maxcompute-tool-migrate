#!/bin/bash
set -e

POSITIONAL=()
TEST="False"

while [[ $# -gt 0 ]]
do
  key=${1}

  case ${key} in
      -t|--test)
      TEST="True"
      shift
      ;;
      *)
      POSITIONAL+=(${1})
      shift
      ;;
  esac
done
set -- "${POSITIONAL[@]}"

echo "Build starts"

echo "${TEST}"

# using double brackets may lead to build break
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
-Dfile=mma-server/src/main/resources/taobao-sdk-java-auto_1479188381469-20200701.jar \
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
cp mma-server/target/mma-server-1.0-SNAPSHOT.jar mma/lib/
cp data-transfer-hive-udtf/target/data-transfer-hive-udtf-1.0-SNAPSHOT-jar-with-dependencies.jar mma/lib
cp mma-client/target/mma-client-1.0-SNAPSHOT.jar mma/lib/
echo "  Done"
echo "  Copy configuration files"
cp  -r build/conf/ mma/conf
echo "  Done"
echo "  Copy resources"
cp -r build/res mma/res
echo "  Done"
if [ ${TEST} == "True" ];
then
  echo "  Copy test"
  cp -r test mma/
  echo "  Done"
  echo "  Prepare test dependencies"
  wget -q https://odps-repo.oss-cn-hangzhou.aliyuncs.com/odpscmd/latest/odpscmd_public.zip
  unzip -q -o odpscmd_public.zip -d mma/res/odpscmd
  rm odpscmd_public.zip
  echo "  Done"
fi
echo "  Package"
tar cpfz mma.tar.gz mma
echo "  Done"
echo "Done"

echo "Clean up"
rm -rf mma
echo "Done"

echo "Build finished"
