from setuptools import setup, find_packages

VERSION = '0.0.12'
DESCRIPTION = 'A simple set of utilities for interacting with Azure'
LONG_DESCRIPTION = 'A simple set of utilities for interacting with Azure'

# Setting up
setup(
    name="simple_azure_utils",
    version=VERSION,
    author="Terrell Mack",
    author_email="terrell.mack@live.com",
    description=DESCRIPTION,
    long_description=LONG_DESCRIPTION,
    packages=find_packages(),
    install_requires=[
        "azure-storage-blob>=12.4.0",
        "azure-identity>=1.4.1"
    ],
        # add any additional packages that

    keywords=['python', 'azure'],
    classifiers=[
        "Development Status :: 3 - Alpha",
        "Intended Audience :: Education",
        "Programming Language :: Python :: 3",
        "Operating System :: MacOS :: MacOS X",
        "Operating System :: Microsoft :: Windows",
    ]
)