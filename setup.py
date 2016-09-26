
import re

try:
    from setuptools import setup, find_packages
except ImportError:
    from distutils.core import setup, find_packages


with open('analytics_utils/__init__.py', 'r') as fd:
    version = re.search(r'^__version__\s*=\s*[\'"]([^\'"]*)[\'"]',
                        fd.read(), re.MULTILINE).group(1)

if not version:
    raise RuntimeError('Cannot find version information')

setup(
    # Application name:
    name="analytics_utils",

    # Version number (initial):
    version=version,

    # Application author details:
    author="Julien Brayere",
    author_email="julien.brayere@veinteractive.com",
    #
    # Packages
    packages=find_packages(),
    #
    # Include additional files into the package
    include_package_data=True,
    #
    # # Details
    # url="http://pypi.python.org/pypi/MyApplication_v010/",
    #
    # #
    # # license="LICENSE.txt",
    # description="Useful towel-related stuff.",
    #
    # # long_description=open("README.txt").read(),
    # # Dependent packages (distributions)
    install_requires=[
        "coloredlogs",
        "sqlalchemy",
        "sqlalchemy_utils",
        "enum34"
    ],
)
