#!/bin/bash
# This script closes all nodes.

NODE_BINARY="node"

echo "Closing all nodes..."
pkill -f "cargo run --bin $NODE_BINARY"

if [ $? -eq 0 ]; then
    echo "All nodes have been closed successfully."
else
    echo "No nodes were found running, or an error occurred."
fi
