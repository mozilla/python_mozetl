import click
from mozetl.churn import churn


@click.group()
def entry_point():
    pass


entry_point.add_command(churn.main, "churn")