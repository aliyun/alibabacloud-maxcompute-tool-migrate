#!/bin/zsh

home=`pwd`

cd mma-ui
./build.sh

cd $home

mvn package -pl mma-server -am -Dhive=$1