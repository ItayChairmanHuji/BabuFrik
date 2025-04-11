from typing import Any


class Configuration:
    def __init__(self, config: dict[str, Any], mandatory_fields: list[str]):
        self.config = config["configuration"] if "configuration" in config else {}
        self.analyzers = config["analyzers"] if "analyzers" in config else []
        self.dynamic_fields = config["dynamic_fields"] if "dynamic_fields" in config else {}
        self.name = config["config_name"] if "config_name" in config else None
        self.type = config["service_type"] if "service_type" in config else None
        self.repetitions = config["repetitions"] if "repetitions" in config else 1
        for field in mandatory_fields:
            if field not in self.config and field not in self.dynamic_fields:
                raise KeyError(f"Field {field} is mandatory in this configuration")

    def __getitem__(self, item: str) -> Any:
        return self.config[item]

    def __setitem__(self, key: str, value: Any) -> None:
        self.config[key] = value

    def __contains__(self, item: str) -> bool:
        return item in self.config
