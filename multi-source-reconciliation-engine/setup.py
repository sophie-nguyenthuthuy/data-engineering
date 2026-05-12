from setuptools import setup, find_packages

setup(
    name="multi-source-reconciliation-engine",
    version="1.0.0",
    packages=find_packages(),
    install_requires=[
        "pandas>=2.0.0",
        "numpy>=1.24.0",
        "rapidfuzz>=3.0.0",
        "pyyaml>=6.0",
        "jinja2>=3.1.0",
        "rich>=13.0.0",
        "click>=8.1.0",
        "python-dateutil>=2.8.2",
        "openpyxl>=3.1.0",
    ],
    entry_points={
        "console_scripts": [
            "reconcile=main:cli",
        ],
    },
    python_requires=">=3.10",
)
