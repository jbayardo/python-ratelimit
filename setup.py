# -*- coding: utf-8 -*-
from distutils.core import setup
from setuptools import find_packages

setup(
    name='ratelimit',
    version='0.0.1.dev1',
    author=u'Julián Bayardo',
    author_email='julian@bayardo.com.ar',
    packages=find_packages(exclude=['examples', 'docs', 'tests']),
    url='https://github.com/jbayardo/python-ratelimit',
    license='MIT',
    description='Thread-safe library for rate limiting function calls either in the same machine, or across a network',
    long_description=open('README.md').read(),
    classifiers=[
        'Development Status :: 3 - Alpha',
        'License :: OSI Approved :: MIT License',
        'Programming Language :: Python :: 3',
        'Topic :: Software Development :: Libraries'
    ],
    keywords='rate limit threading redis',
    install_requires=['redis']
)