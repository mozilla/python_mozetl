'''ETL code for the addon_aggregates dataset'''

from pyspark.sql import SparkSession
import pyspark.sql.functions as fun
import json
import urllib
import click

MS_FIELDS = ['client_id',
             'normalized_channel',
             'app_version',
             'locale',
             'sample_id',
             'profile_creation_date']

ADDON_FIELDS = ['addons.addon_id',
                'addons.foreign_install',
                'addons.is_system',
                'addons.is_web_extension',
                'addons.install_day']


def get_test_pilot_addons():
    '''
    Fetches all the live test pilot experiments listed in
    the experiments.json file.
    :return a list of addon_ids
    '''
    url = "https://testpilot.firefox.com/api/experiments.json"
    response = urllib.urlopen(url)
    data = json.loads(response.read())
    all_tp_addons = (
        ["@testpilot-addon"] + [i.get("addon_id") for i in data['results'] if i.get("addon_id")]
        )
    return all_tp_addons


def get_dest(output_bucket, output_prefix, output_version, date=None, sample_id=None):
    '''
    Stitches together an s3 destination.

    :param output_bucket: s3 output_bucket
    :param output_prefix: s3 output_prefix (within output_bucket)
    :param output_version: dataset output_version
    :retrn str ->
    s3://output_bucket/output_prefix/output_version/submissin_date_s3=[date]/sample_id=[sid]
    '''
    suffix = ''
    if date is not None:
        suffix += "/submission_date_s3={}".format(date)
    if sample_id is not None:
        suffix += "/sample_id={}".format(sample_id)
    return 's3://' + '/'.join([output_bucket, output_prefix, output_version]) + suffix + '/'


def load_main_summary(spark, input_bucket, input_prefix, input_version):
    '''
    Loads main_summary from the bucket constructed from
    input_bucket, input_prefix, input_version

    :param spark: SparkSession object
    :param input_bucket: s3 bucket (telemetry-parquet)
    :param input_prefix: s3 prefix (main_summary)
    :param input_version: dataset version (v4)
    :return SparkDF
    '''
    dest = get_dest(input_bucket, input_prefix, input_version)
    print("loading...", dest)
    return (spark
            .read
            .option("mergeSchema", True)
            .parquet(dest))


def ms_explode_addons(ms):
    '''
    Explodes the active_addons object in
    the ms DataFrame and selects relevant fields

    :param ms: a subset of main_summary
    :return SparkDF
    '''
    addons_df = (
        ms
        .select(MS_FIELDS + [fun.explode('active_addons').alias('addons')])
        .select(MS_FIELDS + ADDON_FIELDS)
        .withColumn("app_version", fun.substring("app_version", 1, 2)))
    return addons_df


def add_addon_columns(df):
    '''
    Constructs additional indicator columns decribing the add-on/theme
    present in a given record. The columns are

    is_self_install
    is_shield_addon
    is_foreign_install
    is_system
    is_web_extension
    Which maps True -> 1 and False -> 0


    :param df: SparkDF, exploded on active_addons, each record
               maps to a single add-on
    :return df with the above columns added
    '''
    addons_expanded = (
        df
        .withColumn("is_self_install",
                    fun.when((df.addon_id.isNotNull()) &
                             (~ df.is_system) &
                             (~ df.foreign_install) &
                             (~ df.addon_id.like('%mozilla%')) &
                             (~ df.addon_id.like('%cliqz%')) &
                             (~ df.addon_id.like('%@unified-urlbar%')) &
                             (~ df.addon_id.isin(*NON_MOZ_TEST_PILOT_ADDONS)),
                             1).otherwise(0))
        .withColumn("is_shield_addon",
                    fun.when(df.addon_id.like('%@shield.mozilla%'),
                             1).otherwise(0))
        .withColumn("is_foreign_install",
                    fun.when(df.foreign_install, 1).otherwise(0))
        .withColumn("is_system",
                    fun.when(df.is_system, 1).otherwise(0))
        .withColumn("is_web_extension",
                    fun.when(df.is_web_extension, 1).otherwise(0)))
    return addons_expanded


def aggregate_addons(df):
    '''
    Aggregates add-on indicators by client, channel, version and locale.
    The result is a DataFrame with the additional aggregate columns:

    n_self_installed_addons (int)
    n_shield_addons (int)
    n_foreign_installed_addons (int)
    n_system_addons (int)
    n_web_extensions (int)
    first_addon_install_date (str %Y%m%d)
    profile_creation_date (str %Y%m%d)

    for each of the above facets.

    :param df: an expoded instance of main_summary by active_addons
               with various additional indicator columns
    :return SparkDF: an aggregated dataset with each of the above columns
    '''
    addon_aggregates = (
        df
        .distinct().groupBy("client_id", "normalized_channel", "app_version", "locale")
        .agg(fun.sum("is_self_install").alias("n_self_installed_addons"),
             fun.sum("is_shield_addon").alias("n_shield_addons"),
             fun.sum("is_foreign_install").alias("n_foreign_installed_addons"),
             fun.sum("is_system").alias("n_system_addons"),
             fun.sum("is_web_extension").alias("n_web_extensions"),
             fun.min(fun.when(df.is_self_install == 1,
                              fun.date_format(fun.from_unixtime(fun.col("install_day")*60*60*24),
                                              "yyyyMMdd")).otherwise(None))
             .alias("first_addon_install_date"),
             fun.date_format(
                fun.from_unixtime(
                    fun.min("profile_creation_date")*60*60*24), "yyyyMMdd")
             .alias("profile_creation_date")))
    return addon_aggregates


NON_MOZ_TEST_PILOT_ADDONS = set([i for i in get_test_pilot_addons() if 'mozilla' not in i])
DEFAULT_THEME_ID = "{972ce4c6-7e08-4474-a285-3208198ce6fd}"


@click.command()
@click.option('--date', required=True)
@click.option('--input-bucket', default='telemetry-parquet')
@click.option('--input-prefix', default='main_summary')
@click.option('--input-version', default='v4')
@click.option('--output-bucket', default='telemetry-parquet')
@click.option('--output-prefix', default='addons/agg')
@click.option('--output-version', default='v2')
def main(date, input_bucket, input_prefix, input_version,
         output_bucket, output_prefix, output_version):
    '''
    Loads main_summary where submission_date_s3 == date
    Partition by sample_id and write aggregated data to s3
    '''
    spark = (SparkSession
             .builder
             .appName("addon_aggregates")
             .getOrCreate())
    # don't write _SUCCESS files, which interfere w/ReDash discovery
    spark.conf.set(
        "mapreduce.fileoutputcommitter.marksuccessfuljobs", "false"
    )
    # load main_summary
    ms = load_main_summary(spark, input_bucket, input_prefix, input_version)
    # filter main summary to the date supplied in the environment
    ms_day = ms.filter(ms.submission_date_s3 == date)
    # partition each date into 100 partitions by sample_id
    for sample_id in range(100):
        exploded = ms_explode_addons(ms_day.filter(ms_day.sample_id == sample_id))
        exploded_addons = add_addon_columns(exploded)
        aggregates = aggregate_addons(exploded_addons)
        dest = get_dest(output_bucket, output_prefix, output_version, date, sample_id)
        # 1 partition is ~ 50MB
        aggregates.repartition(1).write.format('parquet').save(dest, mode='overwrite')


if __name__ == '__main__':
    main()
