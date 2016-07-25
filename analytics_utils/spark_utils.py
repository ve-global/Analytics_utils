"""
Utils function.
"""
import sys
import os

from glob import glob


def add_pyspark_path_if_needed(spark_home):
    """Add PySpark to the library path based on the value of SPARK_HOME if
    pyspark is not already in our path"""
    try:
        from pyspark import context
    except ImportError:
        add_pyspark_path(spark_home)


def add_pyspark_path(spark_home):
    """Add PySpark to the library path based on the value of SPARK_HOME."""
    os.environ['SPARK_HOME'] = spark_home
    sys.path.append(os.path.join(spark_home, 'python'))
    py4j_src_zip = glob(os.path.join(spark_home, 'python',
                                     'lib', 'py4j-*-src.zip'))
    if len(py4j_src_zip) == 0:
        raise ValueError('py4j source archive not found in %s'
                         % os.path.join(spark_home, 'python', 'lib'))
    else:
        py4j_src_zip = sorted(py4j_src_zip)[::-1]
        sys.path.append(py4j_src_zip[0])