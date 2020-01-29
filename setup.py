from setuptools import setup

if __name__ == '__main__':
    setup(
        name='cbl_migrator',
        version='0.1.4',
        author='Eloy Félix',
        author_email='eloyfelix@gmail.com',
        description='Migrates Oracle dbs to PostgreSQL, MySQL and Sqlite',
        url='https://github.com/chembl/cbl_migrator',
        license='MIT',
        packages=['cbl_migrator'],
        long_description=open('README.md', encoding='utf-8').read(),
        long_description_content_type='text/markdown',
        install_requires=['SQLAlchemy>=1.3'],
        tests_require=['exrex'],
        classifiers=['Development Status :: 2 - Pre-Alpha',
                     'Intended Audience :: Developers',
                     'License :: OSI Approved :: MIT License',
                     'Operating System :: POSIX :: Linux',
                     'Programming Language :: Python :: 3.6',
                     'Programming Language :: Python :: 3.7',
                     'Programming Language :: Python :: 3.8'],
        zip_safe=True,
    )
