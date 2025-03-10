#!/bin/bash
# This script launches a number of nodes in separate terminals.

BASE_INTERNAL_ADDRESS="127.0.0.1:808"
BASE_CLIENT_ADDRESS="127.0.0.1:809"

SIZE=8

CMD_TERMINAL="xterm"

for i in $(seq 0 $((SIZE - 1))); do
    ip1="${BASE_INTERNAL_ADDRESS}${i}"
    ip2="${BASE_CLIENT_ADDRESS}${i}"
    echo "Initializing node in ${ip1}"
    sleep 1
    $CMD_TERMINAL -hold -e "cargo run --bin node -- $ip1 $ip2"  &
done

echo "All nodes initialized"
