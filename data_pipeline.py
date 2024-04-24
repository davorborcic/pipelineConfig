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

if __name__ == '__main__':
    pipeline = Pipeline("Dependency Test",
                        [
                            PipelineElement(ElType.SOURCE, "CRM"),
                            PipelineElement(ElType.SOURCE, "ERP"),
                            PipelineElement(ElType.SOURCE, "Product Hierarchies"),
                            PipelineElement(ElType.TRANSFORMATION, "Extract Opportunities"),
                            PipelineElement(ElType.TRANSFORMATION, "Extract ERP"),
                            PipelineElement(ElType.TRANSFORMATION, "Extract Product Hierarchies"),
                            PipelineElement(ElType.TABLE, "OPPORTUNITIES STG"),
                            PipelineElement(ElType.TABLE, "CUSTOMERS STG"),
                            PipelineElement(ElType.TABLE, "SALES STG"),
                            PipelineElement(ElType.TABLE, "PRODUCT STG"),
                            PipelineElement(ElType.TRANSFORMATION, "TRANSFORM OPPORTUNITIES"),
                            PipelineElement(ElType.TRANSFORMATION, "TRANSFORM SALES CUSTOMER"),
                            PipelineElement(ElType.TRANSFORMATION, "TRANSFORM PRODUCT"),
                            PipelineElement(ElType.TABLE, "OPPORTUNITIES FACTS"),
                            PipelineElement(ElType.TABLE, "SALES FACTS"),
                            PipelineElement(ElType.TABLE, "CUSTOMER DIM"),
                            PipelineElement(ElType.TABLE, "PRODUCT DIM"),
                            PipelineElement(ElType.REPORT, "Quarterly Sales"),
                            PipelineElement(ElType.REPORT, "Opportunities Pipeline")
                        ],
                        [
                            (0, 3),
                            (1, 4),
                            (2, 5),
                            (3, 6),
                            (4, 7),
                            (4, 8),
                            (5, 9),
                            (6, 10),
                            (7, 11),
                            (8, 11),
                            (9, 12),
                            (10, 13),
                            (11, 14),
                            (11, 15),
                            (12, 16),
                            (13, 17),
                            (14, 17),
                            (16, 17),
                            (15, 18),
                            (16, 18)
                        ]
                        )

    print(pipeline.find_dependencies(1))
    for i in pipeline.find_dependencies(1):
        print(pipeline.vertices[i])
    print(pipeline.trace_lineage(18))
