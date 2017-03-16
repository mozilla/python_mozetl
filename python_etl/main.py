import example_job

def job_group_closure(jobs):
    def run(sc, sqlContext):
        map(lambda func: func(sc, sqlContext), jobs)

    return run

job_periodicity_config = {
    'daily': [
        example_job.etl_job,
    ],
    'weekly': [],
}

job_groups = map(lambda (k, v): (k, job_group_closure(v)),
                 job_periodicity_config.items())
