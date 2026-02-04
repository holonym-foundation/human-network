#!/bin/bash

# Command parameters
BINARY="./human/target/release/cli"
INPUT="usr:123"
PRIVATE_KEY="${TEST_PRIVATE_KEY:-your-test-private-key-here}"
METHOD="OPRFBabyJubJub"
RPC_URL="${TEST_RPC_URL:-http://localhost:8081}"

# Total number of iterations
TOTAL_ITERATIONS=30

# Delay between calls (in seconds, optional)
DELAY=0

# Log directory
LOG_DIR="cli_logs"
mkdir -p "$LOG_DIR"

# Ensure the binary exists
if [ ! -f "$BINARY" ]; then
    echo "Error: Binary $BINARY not found. Please build the project first."
    exit 1
fi

echo "Starting $TOTAL_ITERATIONS sequential calls at $(date)"

# Function to run the CLI command
run_command() {
    local iteration=$1
    local log_file="$LOG_DIR/cli_call_$iteration.log"
    
    echo "Iteration $iteration started at $(date)" > "$log_file"
    
    RUST_LOG=INFO "$BINARY" \
        --input "$INPUT" \
        --private-key "$PRIVATE_KEY" \
        --method "$METHOD" \
        --rpc-url "$RPC_URL" \
        >> "$log_file" 2>&1
    
    local status=$?
    if [ $status -ne 0 ]; then
        echo "Iteration $iteration failed with status $status at $(date)" >> "$log_file"
        return $status
    else
        echo "Iteration $iteration completed at $(date)" >> "$log_file"
        return 0
    fi
}

# Run iterations sequentially
for ((i=1; i<=TOTAL_ITERATIONS; i++))
do
    echo "Running iteration $i of $TOTAL_ITERATIONS"
    run_command "$i"
    
    # Check if the command failed
    if [ $? -ne 0 ]; then
        echo "Error: Command failed on iteration $i, stopping execution"
        exit 1
    fi
    
    # Optional delay between calls (except after the last one)
    if [ $i -lt $TOTAL_ITERATIONS ] && [ $DELAY -gt 0 ]; then
        sleep "$DELAY"
    fi
done

echo "All $TOTAL_ITERATIONS calls completed successfully at $(date)"
