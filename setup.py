#!/usr/bin/env python

from setuptools import setup, find_packages

# https://pythonhosted.org/setuptools/setuptools.html#developer-s-guide
setup(
    name='kraken',
    version = "0.0.1",
    packages=find_packages(),
    license='http://opensource.org/licenses/MIT',
    author='Michael Bumann',
    url='http://gle.solar',
    author_email='hello@michaelbumann.com',
    description='API to receive power data'
)
