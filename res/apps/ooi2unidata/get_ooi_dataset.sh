#!/bin/sh

echo "Starting script..."
ld=$(dirname "$0")
pushd $ld
java -Xmx512M -jar Ooi2Unidata.jar $*
popd
