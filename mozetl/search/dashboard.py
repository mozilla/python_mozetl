from pyspark.sql.functions import explode, col


def search_dashboard_etl(main_summary):
    return main_summary


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
            .withColumn()
    )
