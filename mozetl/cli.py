import click

from mozetl.clientsdaily import rollup as clientsdaily
from mozetl.engagement.churn import job as churn_job
from mozetl.engagement.churn_to_csv import job as churn_to_csv_job
from mozetl.engagement.retention import job as retention_job
from mozetl.experimentsdaily import rollup as experimentsdaily
from mozetl.search import search_rollups
from mozetl.search.aggregates import search_aggregates_click, search_clients_daily_click
from mozetl.sync import bookmark_validation
from mozetl.taar import taar_locale, taar_similarity, taar_legacy


@click.group()
def entry_point():
    pass


entry_point.add_command(churn_job.main, "churn")
entry_point.add_command(churn_to_csv_job.main, "churn_to_csv")
entry_point.add_command(clientsdaily.main, "clients_daily")
entry_point.add_command(experimentsdaily.main, "experiments_daily")
entry_point.add_command(retention_job.main, "retention")
entry_point.add_command(search_aggregates_click, "search_aggregates")
entry_point.add_command(search_clients_daily_click, "search_clients_daily")
entry_point.add_command(search_rollups.main, "search_rollup")
entry_point.add_command(bookmark_validation.main, "sync_bookmark_validation")
entry_point.add_command(taar_locale.main, "taar_locale")
entry_point.add_command(taar_similarity.main, "taar_similarity")
entry_point.add_command(taar_legacy.main, "taar_legacy")

# Kept for backwards compatibility
entry_point.add_command(search_aggregates_click, "search_dashboard")

if __name__ == '__main__':
    entry_point()
