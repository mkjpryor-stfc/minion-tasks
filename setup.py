#!/usr/bin/env python3

import os, re
from setuptools import setup, find_packages

here = os.path.abspath(os.path.dirname(__file__))

with open(os.path.join(here, 'README.md')) as f:
    README = f.read()

if __name__ == "__main__":
    setup(
        name = 'minion-tasks',
        setup_requires = ['setuptools_scm'],
        use_scm_version = True,
        description = 'Utilities for synchronising tasks between different task managers',
        long_description = README,
        classifiers = [
            "Programming Language :: Python",
            "Framework :: Django",
            "Topic :: Internet :: WWW/HTTP",
            "Topic :: Internet :: WWW/HTTP :: WSGI :: Application",
        ],
        author = 'Matt Pryor',
        author_email = 'matt.pryor@stfc.ac.uk',
        url = 'https://github.com/mkjpryor-stfc/minion-tasks',
        keywords = 'task manager sync synchronise synchronize',
        packages = find_packages(),
        include_package_data = True,
        zip_safe = False,
        install_requires = [],
    )
