from distutils.core import setup


VERSION = __import__(analytics_utils).__version__

setup(
    # Application name:
    name="analytics_utils",

    # Version number (initial):
    version=__version__,

    # Application author details:
    author="Julien Brayere",
    author_email="julien.brayere@veinteractive.com",
    #
    # Packages
    packages=["analytics_utils"],
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
        "sqlalchemy",
        "sqlalchemy_utils",
        "enum34"
    ],
)
