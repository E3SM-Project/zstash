from setuptools import find_packages, setup

setup(
    name="zstash",
    version="1.0.1",
    author="Ryan Forsyth, Chris Golaz, Zeshawn Shaheen",
    author_email="forsyth2@llnl.gov, golaz1@llnl.gov, shaheen2@llnl.gov",
    description="Long term HPSS archiving software for E3SM",
    packages=find_packages(exclude=["*.test", "*.test.*", "test.*", "test"]),
    entry_points={
        'console_scripts': [
            'zstash=zstash.main:main'
    ]}
)
