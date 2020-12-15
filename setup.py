#!/usr/bin/env python
from setuptools import setup, find_packages

test_deps = [
    'coverage==4.5.2',
    'pytest-cov==2.6.0',
    'pytest-timeout==1.3.3',
    'moto==1.3.16',
    'mock==2.0.0',
    'pytest==3.10.1',
    'flake8==3.8.4'
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
        'numpy==1.19.4',
        'pandas==1.1.4',
        'pyspark==2.3.2',
        'python_moztelemetry==0.10.2',
        'requests-toolbelt==0.9.1',
        'requests==2.25.0',
        'scipy==1.5.4',
        'typing==3.6.4',
        'six==1.15.0',
    ],
    tests_require=test_deps,
    extras_require=extras,
)
