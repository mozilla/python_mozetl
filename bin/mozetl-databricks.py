#!/usr/bin/env python2

import argparse
import httplib
import os
import json
import logging


def run_submit(args, instance="dbc-caf9527b-e073.cloud.databricks.com"):
    config = {
        "run_name": "mozetl local submission",
        "new_cluster": {
            "spark_version": "4.3.x-scala2.11",
            "node_type_id": "c3.4xlarge",
            "num_workers": args.num_workers,
            "aws_attributes": {
                "availability": "ON_DEMAND",
                "instance_profile_arn": "arn:aws:iam::144996185633:instance-profile/databricks-ec2",
            },
        },
        "spark_python_task": {
            "python_file": "s3://telemetry-airflow/steps/mozetl_runner.py",
            "parameters": args.command,
        },
        "libraries": {
            "pypi": {
                "package": "git+{path}@{branch}".format(
                    path=args.git_path, branch=args.git_branch
                )
            }
        },
    }

    if args.python == 3:
        config["new_cluster"]["spark_env_vars"] = {
            "PYSPARK_PYTHON": "/databricks/python3/bin/python3"
        }

    logging.debug(json.dumps(config, indent=2))

    # https://docs.databricks.com/api/latest/jobs.html#runs-submit
    conn = httplib.HTTPSConnection(instance)
    headers = {
        "Authorization": "Bearer {token}".format(token=args.token),
        "Content-Type": "application/json",
    }
    conn.request("POST", "/api/2.0/jobs/runs/submit", json.dumps(config), headers)
    resp = conn.getresponse()
    logging.info("status: {} reason: {}".format(resp.status, resp.reason))
    logging.info(resp.read())
    resp.close()


def parse_arguments():
    parser = argparse.ArgumentParser(description="run mozetl")
    parser.add_argument(
        "--git-path",
        type=str,
        default="https://github.com/mozilla/python_mozetl.git",
        help="The URL to the git repository e.g. https://github.com/mozilla/python_mozetl.git",
    )
    parser.add_argument(
        "--git-branch", type=str, default="master", help="The branch to run e.g. master"
    )
    parser.add_argument(
        "--num-workers",
        type=int,
        default=2,
        help="Number of worker instances to spawn in the cluster",
    )
    parser.add_argument(
        "--python",
        type=int,
        choices=[2, 3],
        default=2,
        help="Version of Python to run on the cluster",
    )
    parser.add_argument(
        "--token",
        type=str,
        required=True,
        help="A Databricks authorization token, generated from the user settings page",
    )
    parser.add_argument(
        "command", nargs=argparse.REMAINDER, help="Arguments to pass to mozetl"
    )
    args = parser.parse_args()
    return args


if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG)

    args = parse_arguments()
    run_submit(args)
