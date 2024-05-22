#!/usr/bin/env python
# -*- coding: utf-8 -*-

from setuptools import setup, find_packages

with open('README.md') as readme_file:
    readme = readme_file.read()


with open('requirements.txt') as requirements_file:
    requirements = [r for r in requirements_file.readlines()]
    # requirements.pop(0)

# requirements = []

test_requirements = [
]
setup(
    name='event_service_utils',
    version='1.7.1',
    description="Event service utils",
    long_description=readme,
    author="Felipe Arruda Pontes",
    author_email='felipe.arruda.pontes@insight-centre.org',
    packages=find_packages(),
    package_dir={'event_service_utils':
                 'event_service_utils'},
    include_package_data=True,
    install_requires=requirements,
    zip_safe=False,
    license='Apache License, Version 2.0',
    test_suite='tests',
    tests_require=test_requirements
)
