# -*- coding: utf-8 -*-
#
# Copyright (C) 2015-2019 Bitergia
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
# along with this program. If not, see <http://www.gnu.org/licenses/>.
#
# Authors:
#     Santiago Dueñas <sduenas@bitergia.com>
#

import codecs
import os
import re

# Always prefer setuptools over distutils
from setuptools import setup

here = os.path.abspath(os.path.dirname(__file__))
readme_md = os.path.join(here, 'README.md')
version_py = os.path.join(here, 'grimoire_elk', '_version.py')

# Get the package description from the README.md file
with codecs.open(readme_md, encoding='utf-8') as f:
    long_description = f.read()

with codecs.open(version_py, 'r', encoding='utf-8') as fd:
    version = re.search(r'^__version__\s*=\s*[\'"]([^\'"]*)[\'"]',
                        fd.read(), re.MULTILINE).group(1)

setup(name="grimoire-elk",
      description="GrimoireLab library to produce indexes for ElasticSearch",
      long_description=long_description,
      long_description_content_type='text/markdown',
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
      packages=['grimoire_elk', 'grimoire_elk.enriched', 'grimoire_elk.raw', 'grimoire_elk.identities'],
      package_dir={'grimoire_elk.enriched': 'grimoire_elk/enriched'},
      package_data={'grimoire_elk.enriched': ['mappings/*.json']},
      python_requires='>=3.4',
      setup_requires=['wheel'],
      extras_require={'sortinghat': ['sortinghat'],
                      'mysql': ['PyMySQL']},
      tests_require=['httpretty==0.8.6'],
      test_suite='tests',
      scripts=["utils/p2o.py", "utils/gelk_mapping.py"],
      install_requires=[
          'perceval>=0.9.6',
          'perceval-mozilla>=0.1.4',
          'perceval-opnfv>=0.1.2',
          'perceval-puppet>=0.1.4',
          'perceval-finos>=0.1.0',
          'cereslib>=0.1.0',
          'grimoirelab-toolkit>=0.1.4',
          'sortinghat>=0.6.2',
          'graal>=0.2.2',
          'elasticsearch==6.3.1',
          'elasticsearch-dsl==6.3.1',
          'requests==2.21.0',
          'urllib3==1.24.3',
          'PyMySQL==0.9.3',
          'pandas>=0.22.0,<=0.25.3',
          'numpy<=1.18.3',
          'geopy>=2.0.0',
          'statsmodels >= 0.9.0'
      ],
      zip_safe=False
      )
