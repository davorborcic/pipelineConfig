from data_pipeline import Pipeline, PipelineElement, ElType

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
