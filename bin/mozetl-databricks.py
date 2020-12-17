#!/usr/bin/env python2

import argparse
import ast
import json
import logging
from base64 import b64encode
from textwrap import dedent

try:
    from urllib.request import urlopen, Request
except ImportError:
    from urllib2 import urlopen, Request


def api_request(instance, route, data, token):
    api_endpoint = "https://{instance}/{route}".format(
        instance=instance, route=route.lstrip("/")
    )
    headers = {
        "Authorization": "Bearer {token}".format(token=token),
        "Content-Type": "application/json",
    }
    req = Request(api_endpoint, data=data.encode(), headers=headers)
    resp = urlopen(req)
    logging.info("status: {} info: {}".format(resp.getcode(), resp.info()))
    return resp


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
        "contents": b64encode(dedent(runner_data).encode()).decode(),
        "overwrite": True,
        "path": "/FileStore/airflow/{module}_runner.py".format(module=module_name),
    }
    logging.debug(json.dumps(request, indent=2))
    resp = api_request(instance, "/api/2.0/dbfs/put", json.dumps(request), token)
    logging.info(resp.read())


def run_submit(args):
    config = {
        "run_name": "mozetl local submission",
        "new_cluster": {
            "spark_version": "4.3.x-scala2.11",
            "node_type_id": args.node_type_id,
            "num_workers": args.num_workers,
            "aws_attributes": {
                "first_on_demand": 1,
                "availability": args.aws_availability,
                "instance_profile_arn": "arn:aws:iam::144996185633:instance-profile/databricks-ec2",
                "spot_bid_price_percent": 100,
            },
        },
        "spark_python_task": {
            "python_file": "dbfs:/FileStore/airflow/{module}_runner.py".format(
                module=args.module_name
            ),
            "parameters": args.command,
        },
        "libraries": [
            {
                "pypi": {
                    "package": "git+{path}@{branch}".format(
                        path=args.git_path, branch=args.git_branch
                    )
                }
            }
        ],
    }

    if args.python == 3:
        config["new_cluster"]["spark_env_vars"] = {
            "PYSPARK_PYTHON": "/databricks/python3/bin/python3"
        }

    if len(args.pypi_libs) > 0:
        config["libraries"].extend(
            [{"pypi": {"package": lib}} for lib in args.pypi_libs]
        )

    if args.autoscale:
        # Autoscale from 1 worker up to num_workers
        num_workers = config["new_cluster"]["num_workers"]
        config["new_cluster"]["autoscale"] = {
            "min_workers": 1,
            "max_workers": num_workers,
        }

        # Delete the num_workers option as it's mutually exclusive
        # with autoscaling
        del config["new_cluster"]["num_workers"]

    if args.spot_price_percent != 100:
        config["new_cluster"]["aws_attributes"][
            "spot_bid_price_percent"
        ] = args.spot_price_percent

    logging.info(json.dumps(config, indent=2))

    # https://docs.databricks.com/api/latest/jobs.html#runs-submit
    resp = api_request(
        args.instance, "/api/2.0/jobs/runs/submit", json.dumps(config), args.token
    )
    logging.info(resp.read())


def parse_arguments():
    parser = argparse.ArgumentParser(description="run mozetl")

    def coerce_pypi_names(additional_arg):
        """
        This safely parses pypi package imports using the ast module
        """

        class customAction(argparse.Action):
            def __call__(self, parser, args, values, option_string=None):

                coerced_values = []
                try:
                    coerced_values = ast.literal_eval(values)
                    for package in coerced_values:
                        # Do some basic checks that this looks like a
                        # pypi package
                        if (
                            not isinstance(package, str)
                            or len(package.split("==")) != 2
                        ):
                            raise ValueError(
                                "Invalid package list spec: {}".format(values)
                            )
                except Exception:
                    raise
                setattr(args, self.dest, coerced_values)

        return customAction

    parser.add_argument(
        "--git-path",
        type=str,
        default="https://github.com/mozilla/python_mozetl.git",
        help="The URL to the git repository e.g. https://github.com/mozilla/python_mozetl.git",
    )
    parser.add_argument(
        "--git-branch", type=str, default="main", help="The branch to run e.g. main"
    )
    parser.add_argument(
        "--node-type-id", type=str, default="c3.4xlarge", help="EC2 Node type"
    )
    parser.add_argument(
        "--aws-availability",
        type=str,
        default="ON_DEMAND",
        choices=["ON_DEMAND", "SPOT", "SPOT_WITH_FALLBACK"],
        help="Set the AWS availability type for the cluster",
    )
    parser.add_argument(
        "--spot-price-percent",
        type=int,
        default=100,
        help="Set the bid price for AWS spot instances",
    )

    parser.add_argument(
        "--num-workers",
        type=int,
        default=2,
        help="Number of worker instances to spawn in the cluster",
    )
    parser.add_argument("--autoscale", action="store_true", help="Enable autoscale")
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
        "--pypi-libs",
        type=str,
        default="",
        help="""PyPI libraries to install. ex: \"['pylib1==0.1', 'pylib2==3.1']\"""",
        action=coerce_pypi_names("pypi_libs"),
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
