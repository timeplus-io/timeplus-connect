import os
import re
from setuptools import setup, find_packages

c_modules = []

try:
    from Cython.Build import cythonize
    from Cython import __version__ as cython_version

    print(f'Using Cython {cython_version} to build cython modules')
    c_modules = cythonize('timeplus_connect/driverc/*.pyx', language_level='3str')
except ImportError as ex:
    print('Cython Install Failed, Not Building C Extensions: ', ex)
    cythonize = None
except Exception as ex:  # pylint: disable=broad-exception-caught
    print('Cython Build Failed, Not Building C Extensions: ', ex)
    cythonize = None


def run_setup(try_c: bool = True):
    if try_c:
        kwargs = {
            'ext_modules': c_modules,
        }
    else:
        kwargs = {}

    project_dir = os.path.abspath(os.path.dirname(__file__))
    with open(os.path.join(project_dir, 'README.md'), encoding='utf-8') as read_me:
        long_desc = read_me.read()

    version = 'development'
    if os.path.isfile('.dev_version'):
        with open(os.path.join(project_dir, '.dev_version'), encoding='utf-8') as version_file:
            version = version_file.readline()
    else:
        with open(os.path.join(project_dir, 'timeplus_connect', '__version__.py'), encoding='utf-8') as version_file:
            file_version = version_file.read().strip()
            match = re.search(r"version\s*=\s*'(.+)'", file_version)
            if match is None:
                raise ValueError(f'invalid version {file_version} in timeplus_connect/__version__.py')
            version = match.group(1)

    setup(
        name='timeplus-connect',
        author='Timeplus Inc.',
        author_email='eng@timeplus.com',
        keywords=['timeplus', 'superset', 'sqlalchemy', 'http', 'driver'],
        description='Timeplus Database Driver for Python, Pandas, and Superset',
        version=version,
        long_description=long_desc,
        long_description_content_type='text/markdown',
        package_data={'timeplus_connect': ['VERSION', 'py.typed']},
        url='https://github.com/timeplus-io/timeplus-connect',
        packages=find_packages(exclude=['tests*']),
        python_requires='~=3.8',
        license='Apache License 2.0',
        install_requires=[
            'certifi',
            'urllib3>=1.26',
            'pytz',
            'zstandard',
            'lz4'
        ],
        extras_require={
            'sqlalchemy': ['sqlalchemy>1.3.21,<2.0'],
            'numpy': ['numpy'],
            'pandas': ['pandas'],
            'arrow': ['pyarrow'],
            'orjson': ['orjson'],
            'tzlocal': ['tzlocal>=4.0'],
        },
        tests_require=['pytest'],
        entry_points={
            'sqlalchemy.dialects': ['timeplus.connect=timeplus_connect.cc_sqlalchemy.dialect:TimeplusDialect',
                                    'timeplus=timeplus_connect.cc_sqlalchemy.dialect:TimeplusDialect'],
            'superset.db_engine_specs': ['timeplus=timeplus_connect.tp_superset.db_engine_spec:TimeplusEngineSpec']
        },
        classifiers=[
            'Development Status :: 4 - Beta',
            'Intended Audience :: Developers',
            'License :: OSI Approved :: Apache Software License',
            'Programming Language :: Python :: 3.8',
            'Programming Language :: Python :: 3.9',
            'Programming Language :: Python :: 3.10',
            'Programming Language :: Python :: 3.11',
            'Programming Language :: Python :: 3.12',
            'Programming Language :: Python :: 3.13',
        ],
        **kwargs
    )


try:
    run_setup()
# pylint: disable=broad-exception-caught
except (Exception, IOError, SystemExit) as e:
    print(f'Unable to compile C extensions for faster performance due to {e}, will use pure Python')
    run_setup(False)
