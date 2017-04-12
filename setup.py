#!/usr/bin/env python

from setuptools import setup, find_packages
from distutils.core import setup

setup(name='python_etl',
      version='0.1',
      description='Python ETL jobs for Firefox Telemetry to be scheduled on Airflow.',
      author='Ryan Harter',
      author_email='harterrt@mozilla.com',
      url='https://github.com/mozilla/python_etl.git',
      packages=find_packages(exclude=['tests']),
      install_requires=[
          'python_moztelemetry',
          ],
      test_requires=[
          'pytest',
          'coverage',
          'pytest-cov'
          ]
      )
