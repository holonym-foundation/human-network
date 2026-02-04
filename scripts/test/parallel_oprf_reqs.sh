#!/bin/bash

# Command parameters
BINARY="./human/target/release/cli"
INPUT="usr:123"
BASE_PRIVATE_KEY="${TEST_PRIVATE_KEY:-your-test-private-key-here}"
METHOD="OPRFBabyJubJub"
RPC_URL="${TEST_RPC_URL:-http://localhost:8081}"

# Total number of iterations
TOTAL_ITERATIONS=100

# Number of parallel processes
PARALLEL_JOBS=5

# Log directory
LOG_DIR="cli_logs"

# Create log directory with error checking
if ! mkdir -p "$LOG_DIR" 2>/dev/null; then
    echo "Error: Failed to create log directory $LOG_DIR"
    exit 1
fi

# Ensure the binary exists
if [ ! -f "$BINARY" ]; then
    echo "Error: Binary $BINARY not found. Please build the project first."
    exit 1
fi

# Metrics tracking
TOTAL_TIME=0
SUCCESS_COUNT=0
FAILURE_COUNT=0
START_TIME=$(date +%s)

echo "Starting $TOTAL_ITERATIONS calls with $PARALLEL_JOBS parallel jobs at $(date)"

# Function to generate a unique private key based on iteration
generate_private_key() {
    local iteration=$1
    local new_key=$(echo -n "${BASE_PRIVATE_KEY}${iteration}" | sha256sum | cut -d' ' -f1)
    echo "$new_key"
}

# Function to run the CLI command
run_command() {
    local iteration=$1
    local log_file="$LOG_DIR/cli_call_$iteration.log"
    local private_key=$(generate_private_key "$iteration")
    local start_time end_time duration

    # Ensure the log file is created/accessible
    touch "$log_file" 2>/dev/null || {
        echo "Error: Cannot create log file $log_file" >&2
        return 1
    }

    echo "Iteration $iteration started at $(date) with private key: $private_key" > "$log_file"
    start_time=$(date +%s.%N)

    RUST_LOG=INFO "$BINARY" \
        --input "$INPUT=$iteration" \
        --private-key "$private_key" \
        --method "$METHOD" \
        --rpc-url "$RPC_URL" \
        >> "$log_file" 2>&1
    
    local status=$?
    end_time=$(date +%s.%N)
    duration=$(echo "$end_time - $start_time" | bc)
    TOTAL_TIME=$(echo "$TOTAL_TIME + $duration" | bc)

    if [ $status -ne 0 ]; then
        echo "Iteration $iteration failed with status $status at $(date)" >> "$log_file"
        ((FAILURE_COUNT++))
    else
        echo "Iteration $iteration completed in $duration seconds at $(date)" >> "$log_file"
        ((SUCCESS_COUNT++))
    fi
}

# Export the functions and variables
export -f run_command
export -f generate_private_key
export BINARY INPUT BASE_PRIVATE_KEY METHOD RPC_URL LOG_DIR

# Run iterations in batches
for ((i=1; i<=TOTAL_ITERATIONS; i++))
do
    run_command "$i" &

    if (( i % PARALLEL_JOBS == 0 )) || [ $i -eq $TOTAL_ITERATIONS ]; then
        wait
        echo "Batch completed up to iteration $i at $(date)"
    fi
done

END_TIME=$(date +%s)
TOTAL_DURATION=$((END_TIME - START_TIME))
AVERAGE_TIME=$(echo "$TOTAL_TIME / $TOTAL_ITERATIONS" | bc -l)

# Summary log
echo "\nSummary:" > "$LOG_DIR/summary.log"
echo "Total Requests: $TOTAL_ITERATIONS" >> "$LOG_DIR/summary.log"
echo "Successful Requests: $SUCCESS_COUNT" >> "$LOG_DIR/summary.log"
echo "Failed Requests: $FAILURE_COUNT" >> "$LOG_DIR/summary.log"
echo "Total Execution Time: $TOTAL_DURATION seconds" >> "$LOG_DIR/summary.log"
echo "Average Time per Request: $AVERAGE_TIME seconds" >> "$LOG_DIR/summary.log"

echo "All iterations completed at $(date)"
cat "$LOG_DIR/summary.log"
