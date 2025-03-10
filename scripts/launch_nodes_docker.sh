#!/bin/bash
# This script launches a node.

BASE_ADDRESS="127.0.0.1:"
NODE="808"
SIZE=8

CMD_TERMINAL="xterm"    

for i in $(seq 0 $((SIZE - 1))); do
    xterm -hold -e "docker exec -it node-${NODE}${i} /bin/bash -c './node ${BASE_ADDRESS}\$INTERNAL_PORT ${BASE_ADDRESS}\$EXTERNAL_PORT' "&
    sleep 1
done


