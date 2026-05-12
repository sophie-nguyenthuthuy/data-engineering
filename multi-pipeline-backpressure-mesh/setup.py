from setuptools import find_packages, setup

setup(
    name="backpressure-mesh",
    version="0.1.0",
    description="External backpressure coordination mesh for Flink/Spark streaming pipelines",
    packages=find_packages(exclude=["tests*", "examples*"]),
    python_requires=">=3.11",
    install_requires=[],
    extras_require={
        "redis": ["redis[hiredis]>=5.0"],
        "dev": ["pytest>=8.0", "pytest-asyncio>=0.23"],
    },
)
