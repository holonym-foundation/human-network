#!/bin/bash

# Command parameters
BINARY="./network/target/release/cli"
INPUT="usr:123"
PRIVATE_KEY="${TEST_PRIVATE_KEY:-your-test-private-key-here}"
METHOD="OPRFBabyJubJub"
RPC_URL="${TEST_RPC_URL:-http://localhost:8081}"

# Total number of iterations
TOTAL_ITERATIONS=30

# Number of parallel processes
PARALLEL_JOBS=5

# Log directory
LOG_DIR="cli_logs"
mkdir -p "$LOG_DIR"

# Ensure the binary exists
if [ ! -f "$BINARY" ]; then
    echo "Error: Binary $BINARY not found. Please build the project first."
    exit 1
fi

echo "Starting $TOTAL_ITERATIONS calls with $PARALLEL_JOBS parallel jobs at $(date)"

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
    else
        echo "Iteration $iteration completed at $(date)" >> "$log_file"
    fi
}

# Export the function so it can be used by parallel processes
export -f run_command
export BINARY INPUT PRIVATE_KEY METHOD RPC_URL LOG_DIR

# Run iterations in batches of 5
for ((i=1; i<=TOTAL_ITERATIONS; i++))
do
    # Run the command in the background
    run_command "$i" &
    
    # If we've reached the parallel limit or the last iteration, wait for jobs to finish
    if (( i % PARALLEL_JOBS == 0 )) || [ $i -eq $TOTAL_ITERATIONS ]; then
        wait
        echo "Batch completed up to iteration $i at $(date)"
    fi
done

echo "All $TOTAL_ITERATIONS calls completed at $(date)"