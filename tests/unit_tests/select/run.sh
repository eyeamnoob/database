#!/usr/bin/env bash

set -e 

DATABASES_DIR="databases"
TEST_DB_FILE_NAME="testdb.db"
TEST_DB_PATH="${DATABASES_DIR}/${TEST_DB_FILE_NAME}"
BIN_DIR="bin"
EXEC_FILE_NAME="main"
EXEC_FILE_PATH="${BIN_DIR}/${EXEC_FILE_NAME}"

script_dir=$(realpath $(dirname $0))

PROGRAM_OUTPUT_DIR="${script_dir}/output"
PROGRAM_STDOUT="${PROGRAM_OUTPUT_DIR}/stdout.txt"
PROGRAM_STDERR="${PROGRAM_OUTPUT_DIR}/stderr.txt"

if [ -d "${PROGRAM_OUTPUT_DIR}" ]; then
    rm -rf "${PROGRAM_OUTPUT_DIR}"
fi

mkdir -p "${PROGRAM_OUTPUT_DIR}"

cd "${script_dir}/../../.."

input_file="${script_dir}/input.txt"

{
    echo -n "Testing select..." \
    && cat "${input_file}" | "${EXEC_FILE_PATH}" "${TEST_DB_PATH}" > "${PROGRAM_STDOUT}" 2> "${PROGRAM_STDERR}" \
    && printf " [\033[0;32msuccessful\033[0m]\n"
} ||
{
    printf " [\033[0;31mfail\033[0m]\n"
}