import functools
import time
from collections import Counter
import pandas as pd
# from pyspark.sql.functions import udf
# from pyspark.sql.types import MapType, StringType, IntegerType

from analytics_utils.logs import logger

DEFAULT_FMT = '[{name}] {elapsed:0.8f} min'
COLLECT_LIMIT = 5000000


def counter(x):
    return dict(Counter(x))


def get_most_common(x):
    try:
        return Counter(x).most_common()[0][0]
    except IndexError:
        return None

# counter_udf = udf(counter, MapType(keyType=StringType(), valueType=IntegerType()))
# most_common_udf = udf(get_most_common, StringType())


def clock(fmt=DEFAULT_FMT):
    """
    Computes and prints the duration of the function
    :param fmt: format of the time.
    :return:
    """
    def decorate(func):
        @functools.wraps(func)
        def clocked(*args, **kwargs):
            t0 = time.time()
            _result = func(*args, **kwargs)
            elapsed = (time.time() - t0) / 60.
            name = func.__name__
            args = ', '.join(repr(arg) for arg in args)
            result = repr(_result)
            logger.info(fmt.format(**locals()))
            return _result
        return clocked
    return decorate


def to_pd(limit=COLLECT_LIMIT):
    """
    Get a pandas dataframe in a secure way by limiting the size of the output
    :param limit: limit size to return
    :return: a pandas Dataframe
    """
    def decorate(func):
        @functools.wraps(func)
        def limit_size(*args, **kwargs):
            _result = pd.DataFrame(data=[x.asDict() for x in func(*args, **kwargs).take(limit)])
            if _result.shape[0] == limit:
                logger.warning('[{name}] the output is the same size than the limit ({limit})'.format(
                                name=func.__name__, limit=limit))
            return _result
        return limit_size
    return decorate


def take(limit=COLLECT_LIMIT):
    """
       Execute a take in a secure way by limiting the size of the output
       :param limit: limit size to return
       :return: a pandas Dataframe
       """

    def decorate(func):
        @functools.wraps(func)
        def limit_size(*args, **kwargs):
            _result = func(*args, **kwargs).take(limit)
            if len(_result) == limit:
                logger.warning('[{name}] the output is the same size than the limit ({limit})'.format(
                    name=func.__name__, limit=limit))
            return _result

        return limit_size

    return decorate