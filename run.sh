#!/bin/bash

# Get args for running mapreduce
while getopts h:i:m:o: opts; do
   case ${opts} in
        h) host=${OPTARG};;
        i) input=${OPTARG};;
        m) mid=${OPTARG};;
        o) output=${OPTARG};;
   esac
done

echo $host;
echo $input;
echo $mid;
echo $output;

# Stage 1: Mapping and Reducing
echo "Starting Stage 1: Mapping and Reducing..."

# Execute Stage 1 MapReduce job
echo "Running Stage 1 MapReduce job..."
cargo run --bin mrl-ctl -- --host ${host} submit --input ${input} --workload mm-one --output ${mid}

# Stage 2: Mapping and Reducing
echo "Starting Stage 2: Mapping and Reducing..."

# Execute Stage 2 MapReduce job
echo "Running Stage 2 MapReduce job..."
cargo run --bin mrl-ctl -- --host ${host} submit --input ${mid} --workload mm-two --output ${output}

echo "MapReduce job completed successfully."