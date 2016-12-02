"""
Utils function.
"""
import sys
import os
from glob import glob

avro_package = "spark-avro_2.10:2.0.1"

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


def init_spark_py3(notebook_name, spark_home, archive=None, ui_port=4040,
                   with_avro=False, avro_version=None):
    from pyspark import SparkContext
    from pyspark.sql import HiveContext

    archive = archive or "/mnt/home/brayere/miniconda2/envs/pyspark3.zip#pyspark3"

    env_name = archive.split('#')[-1]
    # spark submit
    env_path = "./{env_name}/{env_name}/bin/python".format(env_name=env_name)
    args = '''
        --verbose
        --jars /usr/hdp/current/hadoop-client/hadoop-azure.jar,/usr/hdp/current/hadoop-client/lib/azure-storage-2.2.0.jar
        --master yarn
        --deploy-mode client
        --archives "{archive}"
        --conf spark.ui.port={ui_port}
        --conf spark.yarn.appMasterEnv.PYSPARK_PYTHON={env_path}
        --conf spark.executorEnv.PYTHONHASHSEED=0
        --conf spark.shuffle.service.enabled=true
        --conf spark.dynamicAllocation.enabled=true
        --conf spark.sql.parquet.compression.codec=snappy
        '''.format(archive=archive, env_path=env_path, ui_port=ui_port)

    if with_avro:
        args += '--packages com.databricks:{}\n'.format(avro_version or avro_package)

    os.environ['PYSPARK_SUBMIT_ARGS'] = args + 'pyspark-shell'

    # '--driver-cores 2 --driver-memory 8g ' \
    # '--executor-cores 2 --executor-memory 6g ' \
    #    '--conf spark.dynamicAllocation.maxExecutors={max_executors} ' \

    # spark context
    sc = SparkContext(appName=notebook_name, sparkHome=spark_home)
    sc.setLogLevel('ERROR')

    # sql context
    sql_context = HiveContext(sparkContext=sc)
    return sc, sql_context


def init_spark_py2(notebook_name, spark_home, ui_port=4040,
                   with_avro=False, avro_version=None):
    from pyspark import SparkContext
    from pyspark.sql import HiveContext

    args = '''
        --verbose
        --jars /usr/hdp/current/hadoop-client/hadoop-azure.jar,/usr/hdp/current/hadoop-client/lib/azure-storage-2.2.0.jar
        --master yarn
        --deploy-mode client
        --conf spark.ui.port={ui_port}
        --conf spark.shuffle.service.enabled=true
        --conf spark.dynamicAllocation.enabled=true
        --conf spark.sql.parquet.compression.codec=snappy
        '''.format(ui_port=ui_port)
    if with_avro:
        args += '--packages com.databricks:{}\n'.format(avro_version or avro_package)

    os.environ['PYSPARK_SUBMIT_ARGS'] = args + 'pyspark-shell'

    # spark context
    sc = SparkContext(appName=notebook_name, sparkHome=spark_home)
    sc.setLogLevel('WARN')

    # sql context
    sql_context = HiveContext(sparkContext=sc)
    return sc, sql_context
