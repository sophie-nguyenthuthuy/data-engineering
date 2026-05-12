from setuptools import setup, find_packages

setup(
    name="intelligent-compaction-engine",
    version="1.0.0",
    description="Intelligent Compaction & Partition Pruning Engine for Delta Lake and Iceberg tables",
    author="Compaction Engine Contributors",
    packages=find_packages(),
    python_requires=">=3.9",
    install_requires=[
        "pyspark>=3.4.0",
        "delta-spark>=2.4.0",
        "sqlglot>=18.0.0",
        "schedule>=1.2.0",
        "prometheus-client>=0.17.0",
        "pyyaml>=6.0",
        "rich>=13.0.0",
        "pandas>=2.0.0",
        "pyarrow>=12.0.0",
    ],
    extras_require={
        "iceberg": ["pyiceberg>=0.5.0"],
        "dev": ["pytest>=7.4.0", "pytest-mock>=3.11.0", "freezegun>=1.2.0"],
    },
    entry_points={
        "console_scripts": [
            "compaction-engine=scripts.run_service:main",
            "compaction-benchmark=scripts.benchmark:main",
        ]
    },
)
