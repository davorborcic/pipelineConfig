# Data Pipeline Configurator  

Set of methods and solutions for dealing with task scheduling and dependencies in ML data pipelines

The main module is `data_pipeline.py` and it has 2 classes:

- Pipeline: represents data pipeline (a DAG structure)
- PipelineElement: represents individual elements of the data pipeline, equivalent to a vertix / node in a graph

A _pipeline_ is defined by specifying a list of pipeline elements and then providing edges, indexed (starting with 0) by the sequence in which the elements have been entered. 
  
See `analyze_dependencies.py` for the usage example, for the workflow in the figure below


![data pipeline graph example with numbered elements](/assets/images/Data%20Pipeline.jpg)
Example of the data pipeline workflow, with numbered elements



### References

_Sedgewick, Robert, and Kevin Wayne, Algorithms, Addison-Wesley, 2014_
