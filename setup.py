from distutils.core import setup
from setuptools import find_packages

setup(
    
    name = 'CubETL',
    version = '0.1.0',
    
    author = 'Jose Juan Montes',
    author_email = 'jjmontes@gmail.com',
    
    packages = find_packages(exclude=["*.tests", "*.tests.*", "tests.*", "tests"]),
    
    scripts = ['bin/cubetl'],
    
    url='https://github.com/jjmontesl/cubetl',
    license='LICENSE',
    description='Data manipulation tool (ETL)',
    long_description="CubETL is a data manipulation tool (also known as ETL for Extract, Transform and Load). It allows data processing flows to be defined on a configuration file. It can access and store files, databases, HTTP, FTP, SFTP resources and is extensible and scriptable.",
    
    classifiers = [
        #'Development Status :: 5 - Production/Stable',
        'Intended Audience :: Developers',
        'Intended Audience :: Science/Research',
        #'License :: OSI Approved :: MIT License',
        'Programming Language :: Python',
        'Topic :: Database',
        'Topic :: Scientific/Engineering',
        'Topic :: Utilities'
    ],
    
    install_requires=[
        "repoze.lru >= 0.5.0",
        "sqlalchemy >= 0.7.9",
        "fs >= 0.3.0"
    ],
)

