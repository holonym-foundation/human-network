#!/bin/bash

# Configuration
PARALLEL_JOBS="${1:-110}"  # Default to 5 parallel jobs
TOTAL_ITERATIONS="${2:-400}" # Default to 10 total iterations

# Create the logs directory if it doesn't exist
mkdir -p cli_logs

# Variables
success_count=0
failure_count=0
total_execution_time=0
execution_times=()
declare -A start_times # Associative array to store start time of each process
running_pids=()       # Array to store PIDs of running processes

# Function to generate a random private key (32 bytes hex)
generate_private_key() {
  echo "${TEST_PRIVATE_KEY:-your-test-private-key-here}"
}

# Main loop for total iterations
for i in $(seq 1 "$TOTAL_ITERATIONS"); do
  # Generate random private key and input
  random_private_key=$(generate_private_key)
  random_input="usr:$RANDOM"

  # Construct the command with the generated values
  rpc_url="${TEST_RPC_URL:-http://localhost:8081}"
  command="RUST_LOG=INFO ./network/target/release/cli --input \"$random_input\" --private-key $random_private_key --method OPRFBabyJubJub --rpc-url \"$rpc_url\""

  # Wait if the number of running jobs reaches the configured limit
  while [[ ${#running_pids[@]} -ge "$PARALLEL_JOBS" ]] && [[ ${#running_pids[@]} -gt 0 ]]; do
    pid_to_wait="${running_pids[0]}"
    wait "$pid_to_wait"
    exit_code=$?
    end_time=$(date +%s%N)

    if [[ -v start_times["$pid_to_wait"] ]]; then
      start_time=${start_times["$pid_to_wait"]}
      execution_time=$(( (end_time - start_time) / 1000000 ))
      total_execution_time=$((total_execution_time + execution_time))
      execution_times+=("$execution_time")
      unset start_times["$pid_to_wait"]
      running_pids=("${running_pids[@]:1}") # Remove the first element
      if [[ "$exit_code" -eq 0 ]]; then
        ((success_count++))
      else
        ((failure_count++))
      fi
    fi
  done

  # Execute the command in the background and redirect output to a log file
  start_time=$(date +%s%N) # Record start time in nanoseconds
  eval "$command" > cli_logs/request_$i.log 2>&1 &
  pid=$!                   # Get the PID of the background process
  start_times["$pid"]="$start_time" # Store the start time associated with the PID
  running_pids+=("$pid")    # Add the PID to the list of running processes
done

# Wait for any remaining background processes to complete
while [[ ${#running_pids[@]} -gt 0 ]]; do
  pid_to_wait="${running_pids[0]}"
  wait "$pid_to_wait"
  exit_code=$?
  end_time=$(date +%s%N)
  if [[ -v start_times["$pid_to_wait"] ]]; then
    start_time=${start_times["$pid_to_wait"]}
    execution_time=$(( (end_time - start_time) / 1000000 ))
    total_execution_time=$((total_execution_time + execution_time))
    execution_times+=("$execution_time")
    unset start_times["$pid_to_wait"]
    running_pids=("${running_pids[@]:1}") # Remove the first element
    if [[ "$exit_code" -eq 0 ]]; then
      ((success_count++))
    else
      ((failure_count++))
    fi
  fi
done

# Calculate summary statistics
total_requests="$TOTAL_ITERATIONS"
if [[ "$total_requests" -gt 0 ]]; then
  average_execution_time=$((total_execution_time / total_requests))
else
  average_execution_time=0
fi

# Display the summary and redirect it to a log file
{
  echo "---------------- Summary ----------------"
  echo "Total requests: $total_requests"
  echo "Successful requests: $success_count"
  echo "Failed requests: $failure_count"
  echo "Average execution time: $average_execution_time ms"

  # Calculate and display min, max, and median execution times if there are any results
  if [[ ${#execution_times[@]} -gt 0 ]]; then
    # Sort the execution times numerically
    sorted_times=($(printf '%s\n' "${execution_times[@]}" | sort -n))
    min_time="${sorted_times[0]}"                  # First element is the minimum
    max_time="${sorted_times[@]: -1:1}"           # Last element is the maximum
    median_index=$(( (${#sorted_times[@]} - 1) / 2 )) # Index of the median
    median_time="${sorted_times[$median_index]}"  # Get the median value
    echo "Minimum execution time: $min_time ms"
    echo "Maximum execution time: $max_time ms"
    echo "Median execution time: $median_time ms"
  fi

  echo "-----------------------------------------"
} > cli_logs/summary.log

echo "Summary and request logs have been saved to the cli_logs directory."

exit 0