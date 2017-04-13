#!/usr/bin/env python
# coding: utf8

from setuptools import setup

setup(name='pymongo-schema',
      version='0.1',
      description='A schema analyser for MongoDB written in Python',
      packages=['pymongo_schema'],
      install_requires=[
          'pymongo',
          'bson'
      ],
      entry_points={
          'console_scripts': [
              'pymongo-schema = pymongo_schema.export:main',
          ],
      },
      author='Pierre-Alain Jachiet',
      author_email='pajachiet@gmail.com',
      url='https://github.com/pajachiet/pymongo-schema',
      license="GNU General Public License v3.0",
      keywords=['mongo', 'mongodb', 'schema', 'mongo-connector', 'mongo-connector-postgresql'],
      )
