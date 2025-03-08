import json
import os
import uuid
from collections import defaultdict
from typing import Any

import matplotlib.pyplot as plt
from matplotlib.figure import Figure

from src.runner.route_runner import RouteRunner
from src.utils import consts
from src.utils.object_loader import ObjectLoader


def plot_time(figure: Figure, times: dict[str, list[float]]):
    ObjectLoader.save(times, os.path.join(consts.RESOURCES_DIR_PATH, "times.pk"))
    for node_name, node_times in times.items():
        figure.clear()
        ax = figure.subplots(nrows=1, ncols=1)
        ax.plot(sizes_generator(len(node_times)), node_times)
        ax.set_title(f"{node_name} time as function of size")
        ax.set_xlabel("size")
        ax.set_ylabel("time")
        figure.savefig(os.path.join(consts.RESOURCES_DIR_PATH, f"{node_name}_time_graph.png"))


def main():
    task_id = str(uuid.uuid4())
    running_config_file = consts.MAIN_CONFIGURATION_FILE_PATH  ## Change
    running_config = json.load(open(running_config_file))
    working_dir = os.path.join(consts.TASKS_DIR_PATH, task_id)
    os.makedirs(working_dir, exist_ok=True)
    runner = RouteRunner(working_dir)
    route_name = running_config["route_name"]
    fields_to_modify: dict[str, list[Any]] = running_config["fields_to_modify"]
    figure = plt.figure()
    times = defaultdict(list)
    for index, values_to_modify in enumerate(zip(*fields_to_modify.values())):
        dynamic_fields = {key: value for key, value in zip(fields_to_modify.keys(), values_to_modify)}
        route_config = json.load(open(os.path.join(consts.ROUTES_DIR_PATH, f"{route_name}.json")))
        run_working_dir = os.path.join(working_dir, str(index))
        os.makedirs(run_working_dir, exist_ok=True)
        runner.run(run_working_dir, route_config, **dynamic_fields)
        for node_name, node_time in runner.times.items():
            times[node_name].append(node_time)
    plot_time(figure, times)


if __name__ == '__main__':
    main()
