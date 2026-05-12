from setuptools import setup, find_packages

setup(
    name="zero-downtime-pipeline-upgrades",
    version="1.0.0",
    description=(
        "Zero-downtime deployment system for stateful data pipelines: "
        "shadow mode, output divergence comparison, and gradual traffic shifting."
    ),
    author="",
    python_requires=">=3.10",
    packages=find_packages(exclude=["tests*", "examples*"]),
    extras_require={
        "dev": ["pytest>=7.0", "pytest-timeout>=2.1"],
    },
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
        "Topic :: Software Development :: Libraries",
        "Topic :: System :: Distributed Computing",
    ],
)
