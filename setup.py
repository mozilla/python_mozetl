#!/usr/bin/env python
from setuptools import setup, find_packages

test_deps = [
    'coverage',
    'pytest-cov',
    'pytest-timeout',
    'moto',
    'mock',
    'pytest',
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
        'python_moztelemetry',  # TODO: pin version
        'click',
        'requests',
        'arrow',
        'click_datetime',
    ],
    tests_require=test_deps,
    extras_require=extras,
)
