"""
Utils function.
"""
import sys
import os
from glob import glob

avro_package = 'spark-avro_2.10:2.0.1'
csv_package = 'com.databricks:spark-csv_2.10:1.5.0'


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


def init_spark_py3(notebook_name, archive=None, ui_port=4040,
                   packages=None, custom_jars=None):
    from pyspark.sql import SparkSession

    jars = ["/usr/hdp/current/hadoop-client/hadoop-azure.jar",
            "/usr/hdp/current/hadoop-client/lib/azure-storage-2.2.0.jar"]
    if custom_jars:
        jars += custom_jars

    archive = archive or "/mnt/home/brayere/miniconda2/envs/pyspark3.zip#pyspark3"

    env_name = archive.split('#')[-1]
    # spark submit
    env_path = "./{env_name}/{env_name}/bin/python".format(env_name=env_name)
    args = '''
        --verbose
        --jars {jars}
        --master yarn
        --deploy-mode client
        --archives "{archive}"
        --conf spark.ui.port={ui_port}
        --conf spark.yarn.appMasterEnv.PYSPARK_PYTHON={env_path}
        --conf spark.executorEnv.PYTHONHASHSEED=0
        --conf spark.shuffle.service.enabled=true
        --conf spark.dynamicAllocation.enabled=true
        --conf spark.sql.parquet.compression.codec=snappy
        '''.format(jars=','.join(jars), archive=archive, env_path=env_path, ui_port=ui_port)

    packages = packages or []
    if packages:
        args += '--packages {}\n'.format(','.join(packages))

    os.environ['PYSPARK_SUBMIT_ARGS'] = args + 'pyspark-shell'

    spark = (SparkSession
             .builder
             .appName(notebook_name)
             .getOrCreate())
    spark.sparkContext.setLogLevel('WARN')

    return spark


def init_spark_py2(notebook_name, spark_home, ui_port=4040,
                   packages=None, custom_jars=None):
    from pyspark.sql import SparkSession

    jars = ["/usr/hdp/current/hadoop-client/hadoop-azure.jar",
            "/usr/hdp/current/hadoop-client/lib/azure-storage-2.2.0.jar"]
    if custom_jars:
        jars += custom_jars

    args = '''
        --verbose
        --jars {jars}
        --master yarn
        --deploy-mode client
        --conf spark.ui.port={ui_port}
        --conf spark.shuffle.service.enabled=true
        --conf spark.dynamicAllocation.enabled=true
        --conf spark.sql.parquet.compression.codec=snappy
        '''.format(jars=','.join(jars), ui_port=ui_port)
    packages = packages or []
    if packages:
        args += '--packages {}\n'.format(','.join(packages))

    os.environ['PYSPARK_SUBMIT_ARGS'] = args + 'pyspark-shell'

    spark = (SparkSession
             .builder
             .appName(notebook_name)
             .getOrCreate())
    spark.sparkContext.setLogLevel('WARN')

    return spark
