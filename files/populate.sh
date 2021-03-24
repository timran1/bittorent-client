#!/bin/bash

SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"

for peer in 1 2 3
do
    rm -rf ${SCRIPT_DIR}/p${peer}
    mkdir -p ${SCRIPT_DIR}/p${peer}
    for file in 1 2 3
    do
        # head -c 1M </dev/urandom > ${SCRIPT_DIR}/p${peer}/p${peer}-f${file}.dat
        head -c 1K </dev/urandom > ${SCRIPT_DIR}/p${peer}/p${peer}-f${file}.dat
    done
done

echo "Generated files"
