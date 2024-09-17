#!/usr/bin/env python
from setuptools import setup, find_packages

test_deps = [
    'coverage==5.3',
    'pytest-cov==2.6.0',
    'pytest-timeout==1.3.3',
    'moto==1.3.16',
    'mock==2.0.0',
    'pytest==3.10.1',
    # NOTE: this is pinned to 4.0.0 so as to pin importlib to a version that works.
    'flake8==4.0.0'
]

extras = {
    'testing': test_deps,
}

setup(
    name='mozetl',
    version='0.1',
    description='Python ETL jobs for Firefox Telemetry to be scheduled on Airflow.',
    author='Ryan Harter',
    author_email='harterrt@mozilla.com',
    url='https://github.com/mozilla/python_mozetl.git',
    packages=find_packages(exclude=['tests']),
    include_package_data=True,
    # PLEASE pin any dependencies to exact versions, otherwise things might unexpectedly break!!
    install_requires=[
        'arrow==0.10.0',
        'boto==2.49.0',
        'boto3==1.16.20',
        'botocore==1.19.20',
        'click==7.1.2',
        'click_datetime==0.2',
        'google-cloud-storage==2.7.0',
        'numpy==1.19.4',
        'pandas==1.1.4',
        # NOTE: this is pinned to 3.20.3 because protos changed and we can't regenerate them.
        'protobuf==3.20.3',
        'pyspark==3.2.2',
        'python-moztelemetry @ git+http://github.com/mozilla/python_moztelemetry.git@v0.10.2#egg=python-moztelemetry',
        'requests-toolbelt==0.9.1',
        'requests==2.25.0',
        'scipy==1.5.4',
        'typing==3.6.4',
        'six==1.15.0',
    ],
    tests_require=test_deps,
    extras_require=extras,
)
