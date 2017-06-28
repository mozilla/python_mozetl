#!/bin/bash

# abort immediately on any failure
set -e

# if we are not inside the docker container, run this command *inside* the
# docker container
if [ ! -f /.dockerenv ]; then
    docker run -t -i -v $PWD:/python_mozetl mozetl ./runtests.sh "$@"
    exit $?
fi

# Run tests
if [ $# -gt 0 ]; then
    ARGS="$@"
    tox -- "${ARGS}"
else
    tox
fi