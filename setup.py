from setuptools import setup

if __name__ == '__main__':
    setup(
        name='db_migrator',
        version='0.0.1',
        author='Eloy Félix',
        author_email='',
        description='Migrates Oracle dbs to PostgreSQL, MySQL and Sqlite',
        url='',
        license='MIT',
        packages=['db_migrator', ],
        long_description=open('README.md', encoding='utf-8').read(),
        install_requires=[
            'SQLAlchemy>=1.2',
        ],
        include_package_data=True,
        classifiers=['Development Status :: 2 - Pre-Alpha',
                     'Intended Audience :: Developers',
                     'License :: OSI Approved :: MIT License',
                     'Operating System :: POSIX :: Linux',
                     'Programming Language :: Python :: 3.6',
                     'Programming Language :: Python :: 3.7'],
        zip_safe=True,
    )
