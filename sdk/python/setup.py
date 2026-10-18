"""Setup script for lumenqraph Python package."""

from setuptools import setup, find_packages

with open("README.md", "r", encoding="utf-8") as fh:
    long_description = fh.read()

setup(
    name="lumenqraph",
    version="0.1.0",
    author="Lumen Scribe",
    author_email="dev@lumenscribe.com",
    description="Lumenqraph Python SDK — a typed client over the Lumenqraph REST + GraphQL API",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/Lumen-Scribe/Lumenqraph",
    packages=find_packages(),
    classifiers=[
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.10",
        "Programming Language :: Python :: 3.11",
        "Programming Language :: Python :: 3.12",
        "Programming Language :: Python :: 3.13",
        "Programming Language :: Python :: 3.14",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
        "Topic :: Software Development :: Libraries :: Python Modules",
        "Topic :: Internet",
    ],
    python_requires=">=3.10",
    keywords="stellar soroban blockchain dapp",
    project_urls={
        "Bug Reports": "https://github.com/Lumen-Scribe/Lumenqraph/issues",
        "Documentation": "https://github.com/Lumen-Scribe/Lumenqraph#python-sdk",
        "Source Code": "https://github.com/Lumen-Scribe/Lumenqraph/tree/main/sdk/python",
    },
)
