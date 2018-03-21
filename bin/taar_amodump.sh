#!/bin/bash
#
# Bug 1385232 - Submission script for mozetl on EMR
#
# This is a common entry point to reduce the amount of boilerplate needed
# to run a pyspark job on airflow. ETL jobs should expose themselves through the
# mozetl.cli entry point. All necessary context for the job should be available
# though the environment in the following form: MOZETL_${COMMAND}_${OPTION}.
#
# MOZETL_COMMAND:       a command that runs a particular ETL job; this causes all
#                       arguments to be read through the environment
# MOZETL_GIT_PATH:      (optional) path to the repository
# MOZETL_GIT_BRANCH:    (optional) git branch to use
# MOZETL_SPARK_MASTER:  (optional) spark-submit --master (defaults to yarn)
#
# Flags for running this script in development, always has precedence
# -d (dev mode)     use local repository, script must be run from within the git project
# -p (git path)     alternate to MOZETL_GIT_PATH
# -b (git branch)   alternate to MOZETL_GIT_BRANCH; ignored when using -d
# -m (spark master) alternate to MOZETL_SPARK_MASTER; useful for local setups
# -q (quiet)        turn off tracing
#
# Example usage:
#   bash airflow.sh -d -m localhost churn   # feature flags, local machine
#   MOZETL_COMMAND=churn bash airflow.sh    # environment variables, EMR

# bash "strict" mode
set -euo pipefail

is_dev='false'
is_verbose='true'

# https://stackoverflow.com/a/21128172
while getopts 'dp:b:m:q' flag; do
    case ${flag} in
        d) is_dev='true' ;;
        p) MOZETL_GIT_PATH="${OPTARG}" ;;
        b) MOZETL_GIT_BRANCH="${OPTARG}" ;;
        m) MOZETL_SPARK_MASTER="${OPTARG}" ;;
        q) is_verbose='false' ;;
        *) error "Unexpected option ${flag}" ;;
    esac
done

shift $((OPTIND - 1))

if [[ "$is_verbose" = true ]]; then
    set -x
fi

# set script environment variables
MOZETL_ARGS=${MOZETL_COMMAND:-$@}
MOZETL_GIT_PATH=${MOZETL_GIT_PATH:-https://github.com/mozilla/python_mozetl.git}
MOZETL_GIT_BRANCH=${MOZETL_GIT_BRANCH:-master}
MOZETL_SPARK_MASTER=${MOZETL_SPARK_MASTER:-yarn}

# Jupyter is the default driver, execute with python instead
unset PYSPARK_DRIVER_PYTHON

# create a temporary directory for work
workdir=$(mktemp -d -t tmp.XXXXXXXXXX)
function cleanup {
  rm -rf "$workdir"
}
trap cleanup EXIT

# generate the driver script
cat <<EOT >> ${workdir}/runner.py
from mozetl import cli
cli.entry_point(auto_envvar_prefix="MOZETL")
EOT

# clone and build
if [[ "$is_dev" = true ]]; then
    cd $(git rev-parse --show-toplevel)
else
    cd ${workdir}
    git clone ${MOZETL_GIT_PATH} --branch ${MOZETL_GIT_BRANCH}
    cd python_mozetl
fi

pip install requests-toolbelt==0.8.0
pip install typing==3.6.4
pip install .
python setup.py bdist_egg

python mozetl.taar.taar_amodatabase ${MOZETL_ARGS}
