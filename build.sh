#!/bin/zsh

home=`pwd`

sourceType=$1
hiveVersion=$2
buildUI=$3

if [[ -z $sourceType ]]; then
  echo "./build.sh <sourceType> <hiveVersion for hive> <buildUI optinal>"
  exit
fi

if [[ $sourceType == "hive" && -z $hiveVersion ]]; then
    echo "./build.sh hive <hiveVersion for hive> <buildUI optional>"
    exit
fi

ensure() {
    if ! "$@"; then
      echo "command failed: $*";
      exit 1
    fi
}


if [[ ${buildUI:-false} == "true"  ]]; then
  cd mma-ui
  ensure ./build.sh
  cd $home
fi

version=`date +'%Y%m%d%H'`
sed -i ""  -E   "s/mma-version: .+/mma-version: ${version}/g" mma-server/src/main/resources/application.yml

if [[ $sourceType != "hive" ]]; then
  ensure mvn clean package -pl mma-server -am -Dmaven.test.skip=true -DsourceType=${sourceType}
  ensure cp ${home}/mma-server/target/MMAv3.jar ${home}/release/mma-3.1.0-${sourceType}.jar
else
  ensure mvn clean package -pl mma-server -am -Dmaven.test.skip=true -DsourceType=${sourceType} -Dhive=${hiveVersion}
  ensure cp ${home}/mma-server/target/MMAv3.jar ${home}/release/mma-3.1.0-${sourceType}${hiveVersion}.jar
fi

