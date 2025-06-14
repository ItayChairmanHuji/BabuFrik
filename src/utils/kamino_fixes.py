import sys
import time

import pyvacy
import sqlalchemy as sql
import torch
from pandas import Series

from src.utils.pyvacy_fixes import epsilon, make_optimizer_class


def imports_fix() -> None:
    sys.modules['pyvacy.pyvacy'] = pyvacy
    pyvacy.analysis.epsilon = epsilon
    pyvacy.optim.DPAdam = make_optimizer_class(torch.optim.Adam)
    sys.path.append("./kamino")
    time.clock = time.time
    Series.iteritems = Series.items
    execute_func = sql.engine.base.Connection.execute
    def new_execute_func(self, x, *args, **kwargs):
        if isinstance(x, str):
            res = execute_func(self, sql.text(x), *args, **kwargs)
        else:
            res = execute_func(self, x, *args, **kwargs)
        print(f"SQL: {x} -> {res.context}")
        return res
    sql.engine.base.Connection.execute = new_execute_func
