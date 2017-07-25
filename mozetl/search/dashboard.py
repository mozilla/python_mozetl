import click
from pyspark.sql.functions import explode, col


def search_dashboard_etl(main_summary):
    exploded = explode_search_counts(main_summary)
    group_cols = filter(lambda x: x != 'count', exploded.columns)

    return (
        exploded
            .groupBy(group_cols)
            .sum('count')
            .withColumnRenamed('sum(count)', 'search_count')
    )


def explode_search_counts(main_summary):
    exploded_col_name = 'single_search_count'

    def define_sc_column(field):
        return field, col(exploded_col_name + '.' + field)


    return (
        main_summary
            .withColumn(exploded_col_name, explode(col('search_counts')))
            .withColumn(*define_sc_column('engine'))
            .withColumn(*define_sc_column('source'))
            .withColumn(*define_sc_column('count'))
            .drop(exploded_col_name)
            .drop('search_counts')
    )


@click.command()
@click.argument('submission_date')
@click.argument('bucket')
@click.argument('prefix')
@click.option('--input_bucket',
              default='telemetry-parquet',
              help='Bucket of the input dataset')
@click.option('--input_prefix',
              default='main_summary/v4',
              help='Prefix of the input dataset')
def main(start_date, mode, bucket, prefix, input_bucket, input_prefix):
    spark = (
        SparkSession
        .builder
        .appName("search_dashboard_etl")
        .getOrCreate()
    )

    version = 1
    source_path = "s3://{}/{}/submission_date_s3={}".format(
        input_bucket,
        input_prefix,
        submission_date
    )
    output_path = "s3://{}/{}/v{}/submission_date_s3={}".format(
        output_bucket,
        output_prefix,
        version,
        submission_date
    )

    logger.info("Loading main_summary into memory...")
    main_summary = spark.read.parquet(source_path)

    logger.info("Running the search dashboard ETL job...")
    search_dashboard_data = search_dashboard_etl(main_summary)

    logger.info("Saving rollups to: {}".format(output_path))
    search_dashboard.repartition(10).write.save(output_path)
