import inspect
import sys


def load_class(name: str) -> type:
    module_name = __get_module_name(name)
    __import__(module_name)
    module = sys.modules[module_name]
    is_class_in_module = lambda member: inspect.isclass(member) and member.__module__ == module_name
    class_name = inspect.getmembers(module, is_class_in_module)[0][0]
    return getattr(module, class_name)


def __get_module_name(name: str) -> str:
    class_type = name.split('_')[-1]
    return f"src.{class_type}s.{name}"
