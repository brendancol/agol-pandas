from setuptools import setup, find_packages
from codecs import open
from os import path

here = path.abspath(path.dirname(__file__))

with open(path.join(here, 'DESCRIPTION.rst'), encoding='utf-8') as f:
    long_description = f.read()

INSTALL_REQUIRES = [
    'pandas',
    'requests',
]

TESTS_REQUIRES = [
    'nose'
]

setup(
    name='agol-pandas',
    version='0.0.6',
    description='Interface with ArcGIS Online hosted serivces through Pandas Data Frame objects',
    long_description=long_description,
    url='https://github.com/brendancol/agol-pandas',
    author='Brendan Collins',
    author_email='brendancol@gmail.com',
    license='MIT',
    classifiers=[
        'Development Status :: 3 - Alpha',
        'Intended Audience :: Developers',
        'Topic :: Software Development :: Build Tools',
        'License :: OSI Approved :: MIT License',
        'Programming Language :: Python :: 2.7',
    ],
    packages=find_packages(),
    install_requires=INSTALL_REQUIRES,
    tests_require=TESTS_REQUIRES,
    keywords='esri agol pandas',
)
