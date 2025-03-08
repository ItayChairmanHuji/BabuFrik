from dataclasses import dataclass
from time import time


@dataclass
class NodeReport:
    node_name: str
    output_file_path: str
    start_time: time
    end_time: time
