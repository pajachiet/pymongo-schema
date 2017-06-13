#!/usr/bin/env python
# coding: utf8

from setuptools import setup

setup(name='pymongo-schema',
      version='0.3',
      description='A schema analyser for MongoDB written in Python',
      packages=['pymongo_schema'],
      install_requires=[
          'pymongo',
          'pyyaml',
          'docopt',
          'ete3',
          'pandas',
          'xlwt',
          'xlsxwriter',
          'openpyxl'
      ],
      entry_points={
          'console_scripts': [
              'pymongo-schema = pymongo_schema.command_line:main',
          ],
      },
      author='Pierre-Alain Jachiet',
      author_email='pajachiet@gmail.com',
      url='https://github.com/pajachiet/pymongo-schema',
      license="GNU General Public License v3.0",
      keywords=['mongo', 'mongodb', 'schema', 'mongo-connector', 'mongo-connector-postgresql'],
      )
