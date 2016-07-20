import logging
import coloredlogs
import os

logger = logging.getLogger('reporting')
coloredlogs.install(level='DEBUG', fmt="%(asctime)s %(levelname)s %(message)s")

if not logger.handlers:
    file_handler = logging.FileHandler(os.path.join(os.path.dirname(__file__), './tmp/upload.log'))
    file_format = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    file_handler.setFormatter(file_format)
    logger.addHandler(file_handler)


class NullLoger(object):
    """Null implementation od a logger"""
    def debug(*arg, **kwargs):
        print("[DEBUG]: ", arg, kwargs)

    def info(*arg, **kwargs):
        print("[INFO]: ", arg, kwargs)

    def warning(*arg, **kwargs):
        print("[WARNING]: ", arg, kwargs)

    def error(*arg, **kwargs):
        print("[ERROR]: ", arg, kwargs)
