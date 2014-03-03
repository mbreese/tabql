#!/usr/bin/env python

from distutils.core import setup

setup(name='tabql',
      version='0.0.2-dev',
      description='Use SQLite to query tab-delimited text files',
      author='Marcus R. Breese',
      author_email='marcus@breese.com',
      url='http://github.com/mbreese/tabql/',
      packages=['tabql'],
      scripts=['bin/tabql']
     )
