import logging
logging.getLogger("py4j").setLevel(logging.ERROR)
logging.getLogger("analytics_utils").setLevel(logging.INFO)

# exposing package version number
from .version import __version__
