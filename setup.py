import re

from setuptools import setup, find_packages

_version_re = re.compile(r'__version__\s*=\s*\'\s*(\d+\.\d+\.\d+)\s*\'')

with open('fp5dump/__init__.py', 'rb') as f:
    version = _version_re.search(f.read().decode('utf-8')).group(1)

description = 'A tool for dumping the content on FileMaker .fp5 files'


setup(
    name='fp5dump',
    author='Daniel Schwarz',
    author_email='dan@butter.sh',
    version=version,
    license='MIT',
    url='https://github.com/qwesda/Fp5Dump',
    packages=find_packages(),
    description=description,
    long_description=open('README.md').read(),
    install_requires=[
        'psycopg2 >= 2.5.4'
    ],
    entry_points='''
        [console_scripts]
        fp5dump=fp5dump.fp5dump:main
    ''',
    classifiers=[
        'Intended Audience :: Developers',
        'License :: OSI Approved :: BSD License',
        'Operating System :: Unix',
        'Programming Language :: Python',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.4',
        'Topic :: Database'
        'Topic :: Software Development',
        'Topic :: Software Development :: Libraries :: Python Modules',
    ]
)