import json
import os
from collections import defaultdict

import matplotlib.pyplot as plt
from matplotlib.figure import Figure

from src.runner.route_runner import RouteRunner
from src.utils import consts
from src.utils.object_loader import ObjectLoader


def size_generator(index: int) -> int:
    # return 500 * (2 ** index)
    return index + 1


def sizes_generator(num: int) -> list[int]:
    return [size_generator(i) for i in range(num)]


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
    routes = json.load(open(consts.MAIN_CONFIGURATION_FILE_PATH))
    runner = RouteRunner()
    sizes = sizes_generator(10)
    times = defaultdict(list)
    figure = plt.figure()
    for size in sizes:
        for route_name in routes:
            print(f"Running route {route_name} with size {size}")
            route_config = json.load(open(os.path.join(consts.ROUTES_DIR_PATH, f"{route_name}.json")))
            runner.run(route_config, size_to_sample=size, error_in_marginals=0.1 * size)
            for node_name, node_time in runner.times.items():
                times[node_name].append(node_time)
        plot_time(figure, times)


if __name__ == '__main__':
    main()
