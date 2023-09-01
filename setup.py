#!/usr/bin/env python

import setuptools

dependence = ['hy ~= 0.27.0', 'hyrule ~= 0.4.0', 'asyncrule ~= 1.0.0']

setuptools.setup(
    name='structlib',
    version='1.0.0',
    author='vhqr',
    description='more flexible sturct library',
    install_requires=dependence,
    packages=['structlib'],
    package_data={
        'structlib': ['structlib/__init__.py', 'structlib/stream.py']
    },
)
