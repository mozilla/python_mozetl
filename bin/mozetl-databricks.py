#!/usr/bin/env python2

import argparse
import httplib
import os
import json
import logging
from base64 import b64encode
from textwrap import dedent


def generate_runner(module_name, instance, token):
    """Generate a runner for the current module to be run in Databricks."""

    runner_data = """
    # This runner has been auto-generated from mozilla/python_mozetl/bin/mozetl-databricks.py.
    # Any changes made to the runner file will be over-written on subsequent runs.
    from {module} import cli
    
    try:
        cli.entry_point(auto_envvar_prefix="MOZETL")
    except SystemExit:
        # avoid calling sys.exit() in databricks
        # http://click.palletsprojects.com/en/7.x/api/?highlight=auto_envvar_prefix#click.BaseCommand.main
        pass
    """.format(
        module=module_name
    )
    logging.debug(dedent(runner_data))

    request = {
        "contents": b64encode(dedent(runner_data)),
        "overwrite": True,
        "path": "/FileStore/airflow/{module}_runner.py".format(module=module_name),
    }
    logging.debug(json.dumps(request, indent=2))
    conn = httplib.HTTPSConnection(instance)
    headers = {
        "Authorization": "Bearer {token}".format(token=token),
        "Content-Type": "application/json",
    }
    conn.request("POST", "/api/2.0/dbfs/put", json.dumps(request), headers)
    resp = conn.getresponse()
    logging.info("status: {} reason: {}".format(resp.status, resp.reason))
    logging.info(resp.read())
    resp.close()


def run_submit(args):
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
            "python_file": "dbfs:/FileStore/airflow/{module}_runner.py".format(module=args.module_name),
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
    conn = httplib.HTTPSConnection(args.instance)
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
        "--instance",
        type=str,
        default="dbc-caf9527b-e073.cloud.databricks.com",
        help="The Databricks instance.",
    )
    parser.add_argument(
        "--module-name", type=str, default="mozetl", help="Top-level module name to run"
    )
    parser.add_argument(
        "command", nargs=argparse.REMAINDER, help="Arguments to pass to mozetl"
    )
    args = parser.parse_args()
    return args


if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG)

    args = parse_arguments()
    generate_runner(args.module_name, args.instance, args.token)
    run_submit(args)
