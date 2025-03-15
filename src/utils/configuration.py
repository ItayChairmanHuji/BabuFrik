from typing import Any


class Configuration:
    def __init__(self, config: dict[str, Any], mandatory_fields: list[str]):
        self.config = config
        for field in mandatory_fields:
            if field not in self.config or field not in self.config["dynamic_fields"]:
                raise KeyError(f"Field {field} is mandatory in this configuration")

    def __getitem__(self, item: str) -> Any:
        return self.config[item]

    def __setitem__(self, key: str, value: Any) -> None:
        self.config[key] = value

    def __contains__(self, item: str) -> bool:
        return item in self.config
