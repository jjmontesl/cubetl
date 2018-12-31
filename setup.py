from setuptools import setup, find_packages
import cubetl

setup(

    name = 'cubetl',
    #package = 'cubetl',
    version = cubetl.APP_VERSION,

    author = 'Jose Juan Montes',
    author_email = 'jjmontes@gmail.com',

    packages = find_packages(exclude=["*.tests", "*.tests.*", "tests.*", "tests"]),

    include_package_data=True,
    package_data = {
        'cubetl': ['*.template']
    },

    #scripts = ['bin/cubetl'],

    url='https://github.com/jjmontesl/cubetl',
    license='LICENSE.txt',
    description='Data manipulation (ETL) tool and library',
    long_description="CubETL is a framework and related tools for data ETL (Extract, Transform and Load). It allows data processing flows to be defined on a configuration file. It can access and store files, databases, HTTP, FTP, SFTP resources and is extensible and scriptable.",

    classifiers = [
        'Development Status :: 3 - Alpha',
        'Intended Audience :: Developers',
        'Intended Audience :: Science/Research',
        'Intended Audience :: Information Technology',
        'License :: OSI Approved :: MIT License',
        'Programming Language :: Python',
        'Topic :: Database',
        'Topic :: Scientific/Engineering',
        'Topic :: Utilities',
        'Topic :: Text Processing :: Markup :: XML'
    ],

    install_requires = [
        "repoze.lru >= 0.5.0",
        "sqlalchemy >= 0.7.9",
        "fs >= 0.3.0",
        "Jinja2 >= 2.10",
        "pygments >= 2.3.1",
        "simplejson >= 3.16.0",
        "dateutils >= 0.6.6",
        "python-slugify >= 1.2.6",
        "lxml >= 4.2.5",
        "chardet >= 3.0.4",
        "beautifulsoup4>=4.6.3",
        "psutil>=5.4.8",
    ],

    entry_points={'console_scripts': ['cubetl=cubetl.core.bootstrap:main']},
)

