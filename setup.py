from distutils.core import setup

setup(
    # Application name:
    name="appnexus_utils",

    # Version number (initial):
    version="0.1.0",

    # Application author details:
    author="Julien Brayere",
    author_email="julien.brayere@veinteractive.com",
    #
    # Packages
    packages=["appnexus_utils"],
    #
    # # Include additional files into the package
    # include_package_data=True,
    #
    # # Details
    # url="http://pypi.python.org/pypi/MyApplication_v010/",
    #
    # #
    # # license="LICENSE.txt",
    # description="Useful towel-related stuff.",
    #
    # # long_description=open("README.txt").read(),
    #
    # # Dependent packages (distributions)
    install_requires=[
        "coloredlogs",
        "py4j",
        "pyspark",
        "sqlalchemy",
        "sqlalchemy_utils"
    ],
)