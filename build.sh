#!/bin/zsh

home=`pwd`

cd mma-ui
./build.sh

cd $home

mvn clean install -Dhive=$1
cd mma-server
mvn clean package -Dhive=$1
cd target
hash=`md5 -q MMAv3.jar`
hash=${hash:0:5}
cp MMAv3.jar MMAv3_${hash}.jar

cd $home
