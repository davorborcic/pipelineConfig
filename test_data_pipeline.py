from unittest import TestCase

from data_pipeline import Pipeline, PipelineElement, ElType


class TestPipeline(TestCase):
    def setUp(self):
        self.pipeline = Pipeline("Dependency Test",
                                 [
                                     PipelineElement(ElType.SOURCE, "a"),
                                     PipelineElement(ElType.TRANSFORMATION, "b"),
                                     PipelineElement(ElType.TABLE, "c"),
                                     PipelineElement(ElType.REPORT, "d"),
                                     PipelineElement(ElType.REPORT, "e"),
                                     PipelineElement(ElType.REPORT, "f"),
                                     PipelineElement(ElType.REPORT, "g"),
                                     PipelineElement(ElType.REPORT, "g"),
                                 ],
                                 [(1, 2), (1, 3), (1, 4), (2, 4)]
                                 )

    def test_init(self):
        self.assertEqual(self.pipeline.name, "Dependency Test")
        self.assertEqual(self.pipeline.dependencies, {1: [2, 3, 4], 2: [4]})
        self.assertEqual(self.pipeline.vertices[1].element_type, ElType.SOURCE)
        self.assertEqual(self.pipeline.vertices[1].element_name, 'a')
        self.assertEqual(self.pipeline.vertices[3].element_name, 'c')


    def test_add_dependency(self):
        self.fail()

    def test_find_dependencies(self):
        dependencies = self.pipeline.find_dependencies(1)
        self.assertEqual(dependencies, {1, 2, 3, 4})

    def test_trace_lineage(self):
        dependencies = self.pipeline.trace_lineage(4)
        self.assertEqual(dependencies, {4,2,1})


class TestElType(TestCase):

    def test_print(self):
        a = ElType.REPORT
        self.assertEqual(ElType.REPORT.name, "REPORT")
