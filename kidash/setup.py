#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# Copyright (C) 2014-2015 Bitergia
#
# This program is free software; you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation; either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program; if not, write to the Free Software
# Foundation, Inc., 59 Temple Place - Suite 330, Boston, MA 02111-1307, USA.
#
# Authors:
#     Santiago Dueñas <sduenas@bitergia.com>
#     Jesus M. Gonzalez-Barahona <jgb@bitergia.com>
#

import codecs
import os
import re
import sys

# Always prefer setuptools over distutils
from setuptools import setup

here = os.path.abspath(os.path.dirname(__file__))
readme_md = os.path.join(here, 'README.md')
version_py = os.path.join(here, 'kidash', '_version.py')

# Pypi wants the description to be in reStrcuturedText, but
# we have it in Markdown. So, let's convert formats.
# Set up thinkgs so that if pypandoc is not installed, it
# just issues a warning.
try:
    import pypandoc
    long_description = pypandoc.convert(readme_md, 'rst')
except (IOError, ImportError):
    print("Warning: pypandoc module not found, or pandoc not installed. " +
          "Using md instead of rst", file=sys.stderr)
    with codecs.open(readme_md, encoding='utf-8') as f:
        long_description = f.read()

with codecs.open(version_py, 'r', encoding='utf-8') as fd:
    version = re.search(r'^__version__\s*=\s*[\'"]([^\'"]*)[\'"]',
                        fd.read(), re.MULTILINE).group(1)

setup(name="kidash",
      description="GrimoireLab script to manage Kibana dashboards from the command line",
      long_description=long_description,
      url="https://github.com/grimoirelab/GrimoireELK",
      version=version,
      author="Bitergia",
      author_email="metrics-grimoire@lists.libresoft.info",
      license="GPLv3",
      classifiers=[
          'Development Status :: 5 - Production/Stable',
          'Intended Audience :: Developers',
          'Topic :: Software Development',
          'License :: OSI Approved :: GNU General Public License v3 or later (GPLv3+)',
          'Programming Language :: Python :: 3',
          'Programming Language :: Python :: 3.4',
          'Programming Language :: Python :: 3.5'],
      keywords="development repositories analytics",
      packages=['kidash'],
      python_requires='>=3.4',
      setup_requires=['wheel'],
      extras_require={},
      tests_require=[],
      test_suite='tests',
      scripts=["bin/kidash"],
      install_requires=['python-dateutil',
                        'grimoire-elk>=0.30.18',
                        'grimoirelab-panels>=0.0.7'
                        ],
      include_package_data=True,
      zip_safe=False
      )
