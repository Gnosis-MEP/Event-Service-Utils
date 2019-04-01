#!/usr/bin/env python
# -*- coding: utf-8 -*-

from setuptools import setup, find_packages

with open('README.md') as readme_file:
    readme = readme_file.read()


# with open('requirements.txt') as requirements_file:
#     requirements = [r.split('==')[0] for r in requirements_file.readlines()]
#     requirements.pop(0)

requirements = []

test_requirements = [
]
setup(
    name='event_service_utils',
    version='0.1.5',
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
    test_suite='tests',
    tests_require=test_requirements
)
