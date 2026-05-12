from setuptools import setup, find_packages

setup(
    name="ivm-engine",
    version="0.1.0",
    description="Incremental View Maintenance engine — differential dataflow in Python",
    packages=find_packages(),
    python_requires=">=3.9",
    install_requires=[],
    extras_require={"dev": ["pytest>=7.0"]},
)
