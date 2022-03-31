#!/usr/bin/env python

from setuptools import setup, find_packages

import glrestore
from glrestore.version import __version__

setup(name='glrestore',
      version=__version__,
      description='Easily restore objects from AWS glacier from the command line using python',
      url='https://github.com/MrOlm/glrestore',
      author='Matthew Olm',
      author_email='mattolm@gmail.com',
      license='MIT',
      #packages=['inStrain'],
      packages=find_packages(),
      python_requires='>=3.4.0',
      install_requires=[
          'awscli',
          'boto3',
          'awswrangler'
      ],
      entry_points={
            'console_scripts': [
                  'glrestore=glrestore.glrestore:main',
            ],
      },
      zip_safe=True)
