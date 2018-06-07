#!/usr/bin/env python3

import os
from setuptools import setup, find_packages

here = os.path.abspath(os.path.dirname(__file__))

with open(os.path.join(here, 'README.md')) as f:
    README = f.read()

if __name__ == "__main__":
    setup(
        name = 'minion-workflows',
        setup_requires = ['setuptools_scm'],
        use_scm_version = True,
        description = 'Light-weight CLI-based, configurable workflow manager.',
        long_description = README,
        classifiers = [
            "Programming Language :: Python",
            "Framework :: Django",
            "Topic :: Internet :: WWW/HTTP",
            "Topic :: Internet :: WWW/HTTP :: WSGI :: Application",
        ],
        author = 'Matt Pryor',
        author_email = 'matt.pryor@stfc.ac.uk',
        url = 'https://github.com/mkjpryor-stfc/minion-workflows',
        keywords = 'workflow task manager import synchronise synchronize',
        packages = find_packages(),
        include_package_data = True,
        zip_safe = False,
        install_requires = [
            'appdirs',
            'tabulate',
            'click',
            'coolname',
            'jinja2',
            'pyyaml',
            'requests',
        ],
        entry_points = {
            'console_scripts': ['minion = minion.cli.main:main']
        },
    )
