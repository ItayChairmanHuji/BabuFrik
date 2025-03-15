from dataclasses import dataclass


@dataclass
class Message:
    working_dir: str
    input_file: str = None
