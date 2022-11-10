#!/bin/zsh

npm run build && cp -rv dist/* ../mma-server/src/main/resources/static
