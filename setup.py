#!/usr/bin/env python
from setuptools import setup, find_packages

setup(
    name='mozetl',
    version='0.1',
    description='Python ETL jobs for Firefox Telemetry to be scheduled on Airflow.',
    author='Ryan Harter',
    author_email='harterrt@mozilla.com',
    url='https://github.com/mozilla/python_mozetl.git',
    packages=find_packages(exclude=['tests']),
    install_requires=[
        'python_moztelemetry',  # TODO: pin version
        'click'
    ],
)
