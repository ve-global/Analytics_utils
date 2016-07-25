import functools
import time

from sqlalchemy import create_engine
from sqlalchemy_utils import database_exists, create_database

from reporting.logs import logger

DEFAULT_FMT = '[{name}] {elapsed:0.8f} min'
COLLECT_LIMIT = 5000000


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
            _result = func(*args, **kwargs).limit(limit).toPandas()
            if _result.shape[0] == limit:
                 logger.warning('[{name}] the standard dataframe may be larger than the limit ({limit})'.format(
                        name=func.__name__, limit=limit))
            return _result
        return limit_size
    return decorate


def connect_postgres(user, password, host, port, db):
    """
    Connect to a postgresql db. Creates it if it does not exist.

    :param user:
    :param password:
    :param host:
    :param port:
    :param db:
    :return:
    """
    engine = create_engine("postgresql://{user}:{password}@{host}:{port}/{db}".format(
                            user=user, password=password, host=host, port=port, db=db))
    if not database_exists(engine.url):
        create_database(engine.url)
    return engine
