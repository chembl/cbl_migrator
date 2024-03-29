from setuptools import setup

if __name__ == "__main__":
    setup(
        name="cbl_migrator",
        version="0.3.6",
        author="Eloy Félix",
        author_email="efelix@ebi.ac.uk",
        description="Migrates Oracle dbs to PostgreSQL, MySQL and SQLite",
        url="https://github.com/chembl/cbl_migrator",
        license="MIT",
        packages=["cbl_migrator", "cbl_migrator.bin"],
        long_description=open("README.md", encoding="utf-8").read(),
        long_description_content_type="text/markdown",
        install_requires=["SQLAlchemy~=2.0"],
        tests_require=["pytest"],
        entry_points={
            "console_scripts": ["cbl-migrator=cbl_migrator.bin.run_migrator:main"],
        },
        classifiers=[
            "Intended Audience :: Developers",
            "License :: OSI Approved :: MIT License",
            "Programming Language :: Python :: 3.8",
            "Programming Language :: Python :: 3.9",
            "Programming Language :: Python :: 3.10",
            "Programming Language :: Python :: 3.11",
        ],
        zip_safe=True,
    )
