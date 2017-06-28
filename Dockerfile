FROM korekontrol/ubuntu-java-python2

ENV SPARK_VERSION=2.0.2

# install gcc
RUN apt-get update && apt-get install -y g++ libpython-dev libsnappy-dev wget git

# install spark
RUN wget -nv https://d3kbcqa49mib13.cloudfront.net/spark-${SPARK_VERSION}-bin-hadoop2.7.tgz
RUN tar -zxf spark-${SPARK_VERSION}-bin-hadoop2.7.tgz

ENV SPARK_HOME="/spark-${SPARK_VERSION}-bin-hadoop2.7"
ENV PYTHONPATH="${SPARK_HOME}/python/:${SPARK_HOME}/python/lib/py4j-*-src.zip):${PYTHONPATH}"

RUN pip install tox python_moztelemetry

WORKDIR /python_mozetl
COPY . /python_mozetl

# installing dependencies takes the majority of the time
RUN pip install -e .[testing]
