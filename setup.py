from setuptools import find_packages, setup

setup(
    name="zstash",
    version="0.1.0",
    author="Sterling Baldwin",
    author_email="baldwin32@llnl.gov",
    description="A utility to create, update, and extract HPSS tar files",
    scripts=["zstash.py"],
    packages=find_packages())
