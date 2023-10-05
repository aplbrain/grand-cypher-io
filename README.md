# grand-cypher-io

File IO routines for reading and writing OpenCypher files.

---

## Why?

-   To enable the use of OpenCypher files as a standard graph interchange format.
-   To simplify reading and writing in-memory Python graphs to a Neo4j or Neptune database.
-   To serialize and deserialize graphs for long-term (e.g., archival) immutable storage.

## Compatibilities

-   All routines that expect a graph can be run with [Grand](https://github.com/aplbrain/grand) `Graph.nx` objects.
-   You can mock most of a Neo4j database, using this repository for IO and in conjunction with [Grand-Cypher](https://github.com/aplbrain/grand-cypher) for query execution.
-   Designed for use with [AWS Neptune](https://docs.aws.amazon.com/neptune/latest/userguide/bulk-load-tutorial-format-opencypher.html)

## Usage

### Export a graph to OpenCypher-readable files

```python
from grand_cypher_io import graph_to_opencypher_buffers
# `graph` is nx.DiGraph or compatible
vert_buffer, edge_buffer = graph_to_opencypher_buffers(graph)
with open('vertices.csv', 'w') as f:
    f.write(vert_buffer.read())
with open('edges.csv', 'w') as f:
    f.write(edge_buffer.read())
```

### Import a graph from OpenCypher-readable files

```python
from grand_cypher_io import opencypher_buffers_to_graph
with open('vertices.csv', 'r') as f:
    vert_buffer = io.StringIO(f.read())
with open('edges.csv', 'r') as f:
    edge_buffer = io.StringIO(f.read())
graph = opencypher_buffers_to_graph(vert_buffer, edge_buffer)
```

## Usage Considerations

### Edge addition implies vertices

When adding an edge to a graph, the vertices of the edge are also added to the graph. This is counter to the behavior of Neo4j imports, but compatible with the [Grand](https://github.com/aplbrain/grand) graph library assumptions, and greatly reduces the inner-loop complexity of the import process.

Because these implicit vertices have no properties, they are easy to detect and filter out of the graph after importing, if desired.

This behavior also means that it is possible to create a full structural graph from a set of edges alone, without any vertices.

### The `__labels__` magic attribute

Following the [Grand-Cypher](https://github.com/aplbrain/grand-cypher) convention, the `__labels__` attribute is used to store the labels of a node. This is an iterable of strings. The `__labels__` attribute is not required, but if it is present, it will be used to populate the `labels` attribute of the node for the purposes of writing to an OpenCypher file.

Likewise, the `__labels__` attribute is used to populate the `labels` attribute of a node when reading from an OpenCypher file.

<p align='center'><small>Made with 💙 at <a href='http://www.jhuapl.edu/'><img alt='JHU APL' align='center' src='https://user-images.githubusercontent.com/693511/62956859-a967ca00-bdc1-11e9-998e-3888e8a24e86.png' height='42px'></a></small></p>
