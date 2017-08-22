import click
from mozetl.churn import churn
from mozetl.search import dashboard, search_rollups


@click.group()
def entry_point():
    pass


entry_point.add_command(churn.main, "churn")
entry_point.add_command(dashboard.main, "search_dashboard")
entry_point.add_command(search_rollups.main, "search_rollup")
