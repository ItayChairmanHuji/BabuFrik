from time import time
from typing import Callable, Any


def run_with_timer(func: Callable) -> tuple[Any, float]:
    start_time = time()
    output = func()
    end_time = time()
    return output, end_time - start_time
