#!/bin/bash
# This script launches a number of nodes in separated containers.

BASE_ADDRESS="127.0.0.1:"
CMD_TERMINAL="xterm"


docker run --name "node-${1}" --network host \
    -e INTERNAL_PORT="${1}" \
    -e EXTERNAL_PORT="${2}" \
    node:latest \
    tail -f /dev/null &
xterm -hold -e "docker exec -it node-${1} /bin/bash -c './node ${BASE_ADDRESS}${1} ${BASE_ADDRESS}${2}' "&

