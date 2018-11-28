#!/usr/bin/env python
from setuptools import setup, find_packages

test_deps = [
    'coverage',
    'pytest-cov',
    'pytest-timeout',
    'moto==1.3.6',
    'mock',
    'pytest',
    'flake8'
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
    install_requires=[
        'arrow==0.10.0',
        'click==6.7',
        'click_datetime==0.2',
        'numpy==1.13.3',
        'pyspark==2.3.1',
        'pyspark_hyperloglog==2.1.1',
        'python_moztelemetry==0.10.2',
        'requests-toolbelt==0.8.0',
        'requests==2.18.4',
        'scipy==1.0.0rc1',
        'typing==3.6.4'
    ],
    tests_require=test_deps,
    extras_require=extras,
)
