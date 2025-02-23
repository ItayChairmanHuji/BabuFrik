import json
import os
import pickle
from collections import defaultdict
import matplotlib.pyplot as plt

from src.runner.route_runner import RouteRunner
from src.utils import consts


def main():
    routes = json.load(open(consts.MAIN_CONFIGURATION_FILE_PATH))
    runner = RouteRunner()
    sizes = [i for i in range(100, 8100, 100)]
    times = defaultdict(list)
    for size in sizes:
        for route_name in routes:
            print(f"Running route {route_name} with size {size}")
            route_config = json.load(open(os.path.join(consts.ROUTES_DIR_PATH, f"{route_name}.json")))
            runner.run(route_config, size_to_sample=size, error_in_marginals=0.1*size)
            for node_name, node_time in runner.times.items():
                times[node_name].append(node_time)


    with open(os.path.join(consts.RESOURCES_DIR_PATH, "times.pk"), 'wb') as file:
        pickle.dump(type, file, protocol=pickle.HIGHEST_PROTOCOL)

    for node_name, node_times in times.items():
        plt.figure()
        plt.plot(sizes, node_times)
        plt.title(f"{node_name} time as function of size")
        plt.xlabel("size")
        plt.ylabel("time")
        plt.savefig(os.path.join(consts.RESOURCES_DIR_PATH, f"{node_name}_time_graph.png"))
if __name__ == '__main__':
    main()