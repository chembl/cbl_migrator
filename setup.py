from setuptools import setup


if __name__ == '__main__':
    setup(
        name='db_migrator',
        version='0.1',
        author='Eloy Felix',
        author_email='',
        description='Migrate Oracle dbs to PostgreSQL, MySQL and Sqlite',
        url='',
        license='MIT',
        packages=['db_migrator', ],
        long_description=open('README.rst').read(),
        install_requires=[
            'SQLAlchemy>=1.2',
        ],
        include_package_data=True,
        classifiers=['Development Status :: 2 - Pre-Alpha',
                     'Intended Audience :: Developers',
                     'License :: Creative Commons :: Attribution-ShareAlike 3.0 Unported',
                     'Operating System :: POSIX :: Linux',
                     'Programming Language :: Python :: 3.5',
                     'Programming Language :: Python :: 3.6'],
        zip_safe=True,
    )
