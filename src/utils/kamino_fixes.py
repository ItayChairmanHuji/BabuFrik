import sys
import time

import pyvacy
from sqlalchemy import sql

from src.utils.pyvacy_fixes import epsilon


def imports_fix() -> None:
    sys.modules['pyvacy.pyvacy'] = pyvacy
    pyvacy.analysis.epsilon = epsilon
    sys.path.append("./kamino")
    time.clock = time.time
    execute_func = sql.engine.base.Connection.execute
    sql.engine.base.Connection.execute = lambda self, x, *args, **kwargs: (
        execute_func(self, sql.text(x), *args, **kwargs)) if isinstance(x, str) \
        else execute_func(self, x, *args, **kwargs)
