import example_job

def weekly(sc, sqlContext):
    pass

def daily(sc, sqlContext):
    example_job.etl_job(sc, sqlContext)
    example_job.etl_job(sc, sqlContext)
    example_job.etl_job(sc, sqlContext)
    print "carpe diem"

jobs = [example_job.etl_job]*3
