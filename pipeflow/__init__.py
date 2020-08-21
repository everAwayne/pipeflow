# -*- coding: utf-8 -*-

# :copyright: (c) 2020 by Awayne
# :license: Apache 2.0, see LICENSE for more details.

import sys

PY35 = sys.version_info >= (3, 5)
assert PY35, "Require python 3.5 or later version"

from .error import *
from .endpoints.endpoints import *
from .tasks import *
from .server import *

__all__ = [error.__all__ +
           endpoints.endpoints.__all__ +
           tasks.__all__ +
           server.__all__]
