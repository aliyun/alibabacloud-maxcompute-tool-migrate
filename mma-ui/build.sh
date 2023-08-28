#!/bin/zsh

if [ ! -d "node_modules" ]; then
  npm install
fi

static_dir="../mma-server/src/main/resources/static"

if [ ! -d $static_dir ]; then
  echo mkdir $static_dir
  mkdir $static_dir
fi

npm run build && cp -rv dist/* ../mma-server/src/main/resources/static
