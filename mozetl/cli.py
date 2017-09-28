import click
from mozetl.churn import churn
from mozetl.search import dashboard, search_rollups
from mozetl.taar import taar_locale, taar_similarity
from mozetl.sync import bookmark_validation
from mozetl.clientsdaily import rollup as clientsdaily
from mozetl.experimentsdaily import rollup as experimentsdaily
from mozetl.retention import daily_retention


@click.group()
def entry_point():
    pass


entry_point.add_command(churn.main, "churn")
entry_point.add_command(clientsdaily.main, "clients_daily")
entry_point.add_command(experimentsdaily.main, "experiments_daily")
entry_point.add_command(daily_retention.main, "retention")
entry_point.add_command(dashboard.main, "search_dashboard")
entry_point.add_command(search_rollups.main, "search_rollup")
entry_point.add_command(bookmark_validation.main, "sync_bookmark_validation")
entry_point.add_command(taar_locale.main, "taar_locale")
entry_point.add_command(taar_similarity.main, "taar_similarity")

if __name__ == '__main__':
    entry_point()
