#!/bin/bash

# abort immediately on any failure
set -e

# run this script relative to the root of the project
root=$(git rev-parse --show-toplevel)

# if we are not inside the docker container, run this command *inside* the
# docker container
if [ ! -f /.dockerenv ]; then
    docker run -t -i -v ${root}:/python_mozetl mozetl ./bin/run-tests.sh "$@"
    exit $?
fi

# Run tests
if [ $# -gt 0 ]; then
    ARGS="$@"
    tox -- "${ARGS}"
else
    tox
fi