# References
# Sedgewick, Robert, and Kevin Wayne, Algorithms, Addison-Wesley, 2014

from collections import defaultdict
import logging as l
from itertools import count
from enum import Enum


class ElType(Enum):
    """
    Types of data pipeline elements, e.g. data source, database table, transformation routine, database view, report / dashboard
    All elements are technology neutral
    """
    SOURCE = 1,
    TABLE = 2
    TRANSFORMATION = 3
    VIEW = 4
    REPORT = 5


class PipelineElement:

    def __init__(self, el_type, el_name):
        """

        :param el_type: type of data pipeline element (node)
        :param el_name: arbitrary, non-unique name of the element
        """
        self.element_type = el_type
        self.element_name = el_name

    def __str__(self):
        return f"{self.element_name} ({self.element_type.name})"


class Pipeline:
    def __init__(self, name: str, vertices: list, edges: list):
        self.node_id = count(1)
        self.name = name
        self.vertices = {next(self.node_id): i for i in vertices}
        self.dependencies = defaultdict(list)
        self.reverse_dependencies  = defaultdict(list)

        for u, v in edges:
            self.dependencies[u].append(v)
            self.reverse_dependencies[v].append(u)

    def add_dependency(self, vertex_1: PipelineElement, vertex_2: PipelineElement, weight=None):
        self.dependencies[vertex_1].append(vertex_2)

    def find_dependencies(self, node):
        """ Find impact, left-to-right"""
        visited = [False] * len(self.vertices)
        dependents = set()
        dependencies = self.dependencies
        self.dfs(node, visited, dependencies, dependents)
        dependents.remove(node)
        return dependents

    def trace_lineage(self, node):
        """Trace Lineage to be implemented here, right-to-left starting from a leaf"""
        visited = [False] * len(self.vertices)
        dependents = set()
        dependencies = self.reverse_dependencies
        self.dfs(node, visited, dependencies, dependents)
        dependents.remove(node)
        return dependents

    def dfs(self, node, visited, dependencies, dependents):
        visited[node] = True
        dependents.add(node)
        for i in dependencies[node]:
            if not visited[i]:
                self.dfs(i, visited, dependencies, dependents)

