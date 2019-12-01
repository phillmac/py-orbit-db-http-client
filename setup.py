#!/usr/bin/env python

from setuptools import setup, find_packages

version = None
exec(open('orbitdbapi/version.py').read())

setup(
    name='orbitdbapi',
    version=version,
    description='A Python HTTP Orbitdb API Client',
    author='Phillip Mackintosh',
    url='https://github.com/phillmac/py-orbit-db-http-client',
    packages=find_packages(),
    install_requires=[
        'httpx == 0.7.8',
        'sseclient-py >= 1.7'
        ],
    classifiers=[
        'Programming Language :: Python :: 3',
        'License :: OSI Approved :: MIT License',
        'Operating System :: OS Independent',
    ],
)
