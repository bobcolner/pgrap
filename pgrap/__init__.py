'''
High-level functions to access Postgres Capabilities.
'''
__title__ = 'pgrap'
__version__ = '0.5'
__author__ = 'Bob Colner'
__license__ = 'MIT'
__copyright__ = 'Copyright 2016 Bob Colner'

from . import pgrap
from . import pgkv
from . import pgdoc

# Set default logging handler to avoid "No handler found" warnings.
import logging
try:  # Python 2.7+
    from logging import NullHandler
except ImportError:
    class NullHandler(logging.Handler):
        def emit(self, record):
            pass

logging.getLogger(__name__).addHandler(NullHandler())
