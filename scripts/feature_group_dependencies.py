import importlib
import inspect
import os
import sys

from graphviz import Digraph

from feature_catalog.base_feature_group import BaseFeatureGroup


def list_feature_groups(directory: str) -> list[object]:
    classes = []
    sys.path.append(directory)
    for filename in os.listdir(directory):
        if filename.endswith(".py"):
            module_name = filename[:-3]
            module = importlib.import_module(module_name)
            for _, obj in inspect.getmembers(module):
                if inspect.isclass(obj) and issubclass(obj, BaseFeatureGroup) and "feature_catalog." not in str(obj):
                    classes.append(obj)
    sys.path.remove(directory)
    return classes


def main() -> None:
    """Automatically generate a graph showing the dependencies between feature groups."""

    nodes_and_edges = {}
    feature_groups = list_feature_groups("src/feature_catalog/feature_groups/")
    for feature_group in feature_groups:
        nodes_and_edges[feature_group.__name__] = [f.__class__.__name__ for f in feature_group([], "avatarId").depends_on]
   
    dot = Digraph()
    for node in nodes_and_edges:
        dot.node(node)
    for node, neighbours in nodes_and_edges.items():
        for neighbour in neighbours:
            dot.edge(node, neighbour)

    dot.render('docs/images/feature_group_dependencies.gv', format='png')

    
if __name__ == "__main__":
    main()
