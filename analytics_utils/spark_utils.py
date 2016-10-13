"""
Utils function.
"""
import sys
import os
from glob import glob

from pyspark import SparkContext
from pyspark.sql import HiveContext


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


def init_spark_py3(notebook_name, spark_home, archive=None):
    archive = archive or "/mnt/home/brayere/pyspark3.tar.gz#pyspark3"
    # spark submit
    os.environ['PYSPARK_PYTHON'] = "./pyspark3/pyspark3/bin/python"
    os.environ['PYSPARK_SUBMIT_ARGS'] = \
        '--verbose ' \
        '--jars /usr/hdp/current/hadoop-client/hadoop-azure.jar,/usr/hdp/current/hadoop-client/lib/azure-storage-2.2.0.jar ' \
        '--master yarn ' \
        '--deploy-mode client ' \
        '--archives "{archive}" ' \
        '--conf spark.executorEnv.PYTHONHASHSEED=0 ' \
        '--conf spark.shuffle.service.enabled=true ' \
        '--conf spark.dynamicAllocation.enabled=true ' \
        '--conf spark.sql.parquet.compression.codec=snappy ' \
        'pyspark-shell'.format(archive=archive)

    # '--driver-cores 2 --driver-memory 8g ' \
    # '--executor-cores 2 --executor-memory 6g ' \
    #    '--conf spark.dynamicAllocation.maxExecutors={max_executors} ' \

    # spark context
    sc = SparkContext(appName=notebook_name, sparkHome=spark_home)
    sc.setLogLevel('ERROR')

    # sql context
    sql_context = HiveContext(sparkContext=sc)
    return sc, sql_context


def init_spark_py2(notebook_name, spark_home):
    # spark submit
    os.environ['PYSPARK_SUBMIT_ARGS'] = \
        '--verbose ' \
        '--jars /usr/hdp/current/hadoop-client/hadoop-azure.jar,/usr/hdp/current/hadoop-client/lib/azure-storage-2.2.0.jar ' \
        '--master yarn ' \
        '--deploy-mode client ' \
        '--conf spark.shuffle.service.enabled=true ' \
        '--conf spark.dynamicAllocation.enabled=true ' \
        '--conf spark.sql.parquet.compression.codec=snappy ' \
        'pyspark-shell'

    # spark context
    sc = SparkContext(appName=notebook_name, sparkHome=spark_home)
    sc.setLogLevel('WARN')

    # sql context
    sql_context = HiveContext(sparkContext=sc)
    return sc, sql_context
