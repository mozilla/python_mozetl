import click
from mozetl.churn import churn
from mozetl.search import dashboard, search_rollups
from mozetl.taar import taar_locale, taar_similarity
from mozetl.sync import bookmark_validation


@click.group()
def entry_point():
    pass


entry_point.add_command(churn.main, "churn")
entry_point.add_command(dashboard.main, "search_dashboard")
entry_point.add_command(search_rollups.main, "search_rollup")
entry_point.add_command(taar_locale.main, "taar_locale")
entry_point.add_command(taar_similarity.main, "taar_similarity")
entry_point.add_command(bookmark_validation.main, "sync_bookmark_validation")
