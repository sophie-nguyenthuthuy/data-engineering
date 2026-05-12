from setuptools import setup, find_packages

setup(
    name="adaptive-microbatch-optimizer",
    version="0.1.0",
    description="Streaming processor with PID-controlled adaptive micro-batch windows",
    author="",
    python_requires=">=3.11",
    package_dir={"": "src"},
    packages=find_packages(where="src"),
    install_requires=[],
    extras_require={
        "dev": ["pytest>=7.4", "pytest-asyncio>=0.23"],
    },
)
