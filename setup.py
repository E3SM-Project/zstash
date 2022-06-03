from setuptools import find_packages, setup

setup(
    name="zstash",
    version="1.2.1rc1",
    author="Ryan Forsyth, Chris Golaz, Zeshawn Shaheen",
    author_email="forsyth2@llnl.gov, golaz1@llnl.gov, shaheen2@llnl.gov",
    description="Long term HPSS archiving software for E3SM",
    packages=find_packages(include=["zstash", "zstash.*"]),
    install_requires=[
        "six==1.16.0",
        "globus-sdk<4.0.0>=3.0.0",
        "fair-research-login==0.2.6",
    ],
    entry_points={"console_scripts": ["zstash=zstash.main:main"]},
)
