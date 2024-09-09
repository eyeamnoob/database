#!/usr/bin/env bash

set -e

if (( "$#" != "1" )); then
    echo "Please provide only the number of inputs."
    exit 1
fi

script_dir=$(realpath $(dirname $0))
inputs_count="${1}"
input_file="${script_dir}/input.txt"

> "${input_file}"
for((i=1;i<inputs_count+1;i++)); do
    echo "insert ${i} ali${i} ali${i}" >> "${input_file}"
done

echo '.exit' >> "${input_file}"