import click

from mozetl.addon_aggregates import addon_aggregates
from mozetl.clientsdaily import rollup as clientsdaily
from mozetl.engagement.churn import job as churn_job
from mozetl.engagement.churn_to_csv import job as churn_to_csv_job
from mozetl.engagement.retention import job as retention_job
from mozetl.experimentsdaily import rollup as experimentsdaily
from mozetl.landfill import sampler as landfill_sampler
from mozetl.maudau import maudau
from mozetl.search.aggregates import search_aggregates_click, search_clients_daily_click
from mozetl.sync import bookmark_validation
from mozetl.tab_spinner import tab_spinner
from mozetl.taar import (
    taar_locale,
    taar_similarity,
    taar_dynamo,
    taar_amodump,
    taar_amowhitelist,
    taar_lite_guidguid,
    taar_lite_guidranking,
    taar_update_whitelist,
)


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
entry_point.add_command(bookmark_validation.main, "sync_bookmark_validation")
entry_point.add_command(taar_locale.main, "taar_locale")
entry_point.add_command(taar_similarity.main, "taar_similarity")
entry_point.add_command(taar_lite_guidguid.main, "taar_lite")
entry_point.add_command(taar_dynamo.main, "taar_dynamo")
entry_point.add_command(taar_amodump.main, "taar_amodump")
entry_point.add_command(taar_lite_guidranking.main, "taar_lite_guidranking")
entry_point.add_command(taar_amowhitelist.main, "taar_amowhitelist")
entry_point.add_command(taar_update_whitelist.main, "taar_update_whitelist")
entry_point.add_command(addon_aggregates.main, "addon_aggregates")
entry_point.add_command(maudau.main, "engagement_ratio")
entry_point.add_command(landfill_sampler.main, "landfill_sampler")
entry_point.add_command(tab_spinner.main, "long_tab_spinners")

# Kept for backwards compatibility
entry_point.add_command(search_aggregates_click, "search_dashboard")

if __name__ == "__main__":
    entry_point()
