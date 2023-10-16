import itertools
import pathlib
from io import StringIO
import re
from typing import Any, Iterator, List, Tuple, Union

import networkx as nx

FilePointer = Union[str, pathlib.Path]
_FORBIDDEN_ATTR_NAMES = ["__labels__", "__type__"]
_ARRAY_DELIM = ";"


class _TYPES:
    LONG = ["long", "int64", "int32", "int16"]
    INT = ["int", "integer", "int64", "int32", "int16", "int8", int]
    FLOAT = ["float", float]
    STR = ["str", str]
    BOOL = ["bool", "boolean", bool]


def _get_opencypher_dtype(dtype: Any, allow_datetime: bool = False) -> str:
    """
    Convert a Python data type to an openCypher data type.

    Datetime formats follow the Java formats:
        - yyyy-MM-dd
        - yyyy-MM-ddTHH:mm
        - yyyy-MM-ddTHH:mm:ss
        - yyyy-MM-ddTHH:mm:ssZ

    """
    if dtype in _TYPES.BOOL:
        return "Boolean"
    elif dtype in _TYPES.LONG:
        return "Long"
    elif dtype in _TYPES.INT:
        return "Int"
    elif dtype in _TYPES.FLOAT:
        return "Float"
    elif dtype in _TYPES.STR:
        return "String"
    elif dtype in ["date"] and allow_datetime:
        return "DateTime"
    return "String"


def _narrowest_type(type1: Any, type2: Any) -> Any:
    """
    Return the narrowest type that can represent both type1 and type2.

    """
    if type1 == type2:
        return type1
    # If one of the types is a string, the narrowest type is a string:
    if type1 in _TYPES.STR or type2 in _TYPES.STR:
        return str
    # If the types are all numeric, the narrowest type is a float:
    if (
        type1 in _TYPES.INT
        or type2 in _TYPES.INT
        or type1 in _TYPES.FLOAT
        or type2 in _TYPES.FLOAT
    ):
        return float


# def _comma_separated_to_tuple(line: str) -> tuple:
#     """
#     This function separates a CSV line into tuples, respecting escaped commas
#     and quoted values (which may contain commas).

#     """
#     # Naive is to do this:
#     # .strip().split(",")
#     # But that doesn't respect escaped commas or quoted values.
#     # So we'll do it manually, with this speedy regex:
#     # https://stackoverflow.com/a/2787064/979255
#     # return tuple(re.split(r',(?=")', line.strip()))
#     # pattern = re.compile(r"""((?:[^,"']|"[^"]*"|'[^']*')+)""")
#     pattern = re.compile(r"""((?:[^,"']|"[^"]*"|'[^']*')+|(?=,,)|(?=,$)|(?=^,))""")
#     return tuple(pattern.split(line.strip())[1::2])


import csv
import io


def _comma_separated_to_tuple(s: str) -> tuple:
    """Convert a comma-separated string to a tuple.

    This is a helper function for the CSV reader. It handles the case where
    commas are in the values, and also the case where there are empty cells
    (which the CSV reader doesn't handle by default).

    Args:
        s: A string of comma-separated values.

    Returns:
        A tuple of the values.
    """
    # First, we need to handle the case where there are empty cells. The CSV
    # reader doesn't handle this by default, so we need to do it ourselves.
    # We'll use the csv module to do this.
    # First, we need to convert the string to a file-like object.
    # We'll use StringIO for this.
    f = io.StringIO(s)
    # Now we can use the csv module to read the file.
    reader = csv.reader(f)
    # The csv module will handle the empty cells for us.
    # Now we can convert the reader to a tuple.
    return tuple(tuple(reader)[0])


def graph_to_opencypher_buffers(
    graph: nx.Graph,
    default_vertex_label: str = "Vertex",
    default_edge_type: str = "Edge",
    vertex_output_file_or_writeable_buffer: Union[FilePointer, StringIO, None] = None,
    edge_output_file_or_writeable_buffer: Union[FilePointer, StringIO, None] = None,
):
    """
    Convert a networkx graph to openCypher-compatible buffers.

    """
    # Create the vertex buffer if necessary:
    if vertex_output_file_or_writeable_buffer is None:
        vertex_buffer = StringIO()
    elif isinstance(vertex_output_file_or_writeable_buffer, str):
        vertex_buffer = open(vertex_output_file_or_writeable_buffer, "w")
    elif isinstance(vertex_output_file_or_writeable_buffer, pathlib.Path):
        vertex_buffer = open(vertex_output_file_or_writeable_buffer, "w")
    else:
        vertex_buffer = vertex_output_file_or_writeable_buffer

    # Create the edge buffer if necessary:
    if edge_output_file_or_writeable_buffer is None:
        edge_buffer = StringIO()
    elif isinstance(edge_output_file_or_writeable_buffer, str):
        edge_buffer = open(edge_output_file_or_writeable_buffer, "w")
    elif isinstance(edge_output_file_or_writeable_buffer, pathlib.Path):
        edge_buffer = open(edge_output_file_or_writeable_buffer, "w")
    else:
        edge_buffer = edge_output_file_or_writeable_buffer

    # Because we don't know all of the attributes (and they can be different
    # on different vertices), we need to loop through the verts twice; the
    # first time to get all the attributes and their dtypes, and the second
    # time to write the attributes to the file.
    # The only way around this is to load all of the vertices into memory
    # and then loop through them once, but that's not scalable.
    all_vertex_attributes_and_dtypes = {}
    for vertex in graph.nodes:
        for attribute, value in graph.nodes[vertex].items():
            if attribute not in all_vertex_attributes_and_dtypes:
                all_vertex_attributes_and_dtypes[attribute] = type(value)
            else:
                if all_vertex_attributes_and_dtypes[attribute] != type(value):
                    all_vertex_attributes_and_dtypes[attribute] = _narrowest_type(
                        all_vertex_attributes_and_dtypes[attribute], type(value)
                    )
    # We'll keep track of the attributes in sorted order so that the output
    # is consistent across rows:
    sorted_attr_names_and_dtypes = [
        (attribute, _get_opencypher_dtype(dtype))
        for attribute, dtype in sorted(
            all_vertex_attributes_and_dtypes.items(), key=lambda x: x[0]
        )
        if attribute not in _FORBIDDEN_ATTR_NAMES
    ]
    sorted_attr_names = [name for name, dtype in sorted_attr_names_and_dtypes]
    sorted_attr_dtypes = ",".join(
        [f"{name}:{dtype}" for name, dtype in sorted_attr_names_and_dtypes]
    )
    # If there are no attr columns, we still need to write the :ID and :LABEL
    # columns but there's no trailing comma after them:
    has_attrs_comma = "," if sorted_attr_names else ""

    # Create the vertex buffer and write the lines:
    vertex_buffer.write(f":ID,:LABEL{has_attrs_comma}{sorted_attr_dtypes}\n")
    for vertex in graph.nodes:
        sorted_attrs = ",".join(
            [
                # Value if it exists, else empty string
                str(graph.nodes[vertex].get(attribute, ""))
                for attribute in sorted_attr_names
            ]
        )
        vertex_label = _ARRAY_DELIM.join(
            sorted(graph.nodes[vertex].get("__labels__", set([default_vertex_label])))
        )
        vertex_buffer.write(f"{vertex},{vertex_label}{has_attrs_comma}{sorted_attrs}\n")
    vertex_buffer.seek(0)

    # Now we'll do the same thing for the edges:
    all_edge_attributes_and_dtypes = {}
    for edge in graph.edges:
        for attribute, value in graph.edges[edge].items():
            if attribute not in all_edge_attributes_and_dtypes:
                all_edge_attributes_and_dtypes[attribute] = type(value)
            else:
                if all_edge_attributes_and_dtypes[attribute] != type(value):
                    all_edge_attributes_and_dtypes[attribute] = _narrowest_type(
                        all_edge_attributes_and_dtypes[attribute], type(value)
                    )
    # We'll keep track of the attributes in sorted order so that the output
    # is consistent across rows:
    sorted_edge_attr_names_and_dtypes = [
        (attribute, _get_opencypher_dtype(dtype))
        for attribute, dtype in sorted(
            all_edge_attributes_and_dtypes.items(), key=lambda x: x[0]
        )
        if attribute not in _FORBIDDEN_ATTR_NAMES
    ]
    sorted_edge_attr_names = [name for name, dtype in sorted_edge_attr_names_and_dtypes]
    sorted_edge_attr_dtypes = ",".join(
        [f"{name}:{dtype}" for name, dtype in sorted_edge_attr_names_and_dtypes]
    )
    # If there are no attr columns, we still need to write the :ID and :LABEL
    # columns but there's no trailing comma after them:
    has_edge_attrs_comma = "," if sorted_edge_attr_names else ""
    edge_buffer.write(
        f":START_ID,:END_ID,:TYPE{has_edge_attrs_comma}{sorted_edge_attr_dtypes}\n"
    )
    for edge in graph.edges:
        edge_label = graph.edges[edge].get("__type__", default_edge_type)
        edge_buffer.write(
            f"{edge[0]},{edge[1]},{edge_label}{has_edge_attrs_comma}"
            + ",".join(
                [
                    # Value if it exists, else empty string
                    str(graph.edges[edge].get(attribute, ""))
                    for attribute in sorted_edge_attr_names
                ]
            )
            + "\n"
        )
    edge_buffer.seek(0)

    return vertex_buffer, edge_buffer


def opencypher_iterators_to_graph(
    vertex_iterator: Iterator[Tuple],
    edge_iterator: Iterator[Tuple],
    vertex_attributes: Union[List[str], None] = None,
    edge_attributes: Union[List[str], None] = None,
    to_graph: Union[nx.Graph, None] = None,
):
    """
    Construct a graph from value tuples.

    The first value in each tuple is the vertex ID.
    The second value in each vertex tuple is the vertex Label.
    All other values in each vertex tuple are the vertex attributes.

    The first two values in each edge tuple are the source and target vertex
    IDs. The third value is the edge Label. If the edge attributes are provided
    the remaining values are the edge attributes.

    """
    graph = to_graph if to_graph is not None else nx.DiGraph()

    # Create a set of vertex attribute names to use as the column names:
    if vertex_attributes is None:
        vert_attributes_and_types = []
    else:
        vert_attributes_and_types = [
            attribute.split(":") for attribute in vertex_attributes  # name, type
        ]
    for vertex in vertex_iterator:
        if len(vertex) < 2:
            raise ValueError(
                f"Vertex tuples must have at least two values, but got:\n{vertex}"
            )
        vert_id, vert_type = vertex[0], vertex[1]
        vert_attrs = vertex[2:] if len(vertex) > 2 else []
        # Add the vertex to the graph:
        graph.add_node(
            vert_id,
            __labels__=vert_type,
            **{
                attribute: value
                for (attribute, attr_type), value in zip(
                    vert_attributes_and_types, vert_attrs
                )
            },
        )

    # Create a set of edge attribute names to use as the column names:
    if edge_attributes is None:
        edge_attributes_and_types = []
    else:
        edge_attributes_and_types = [
            attribute.split(":") for attribute in edge_attributes  # name, type
        ]
    for edge in edge_iterator:
        if len(edge) < 3:
            raise ValueError(
                f"Edge tuples must have at least three values, but got:\n{edge}"
            )
        edge_source, edge_target, edge_type = edge[0], edge[1], edge[2]
        edge_attrs = edge[3:] if len(edge) > 3 else []
        # Add the edge to the graph:
        graph.add_edge(
            edge_source,
            edge_target,
            __type__=edge_type,
            **{
                attribute: value
                for (attribute, attr_type), value in zip(
                    edge_attributes_and_types, edge_attrs
                )
            },
        )

    return graph


def _get_nbuffer_header_and_tuple_iterator(
    in_buffer: Union[FilePointer, StringIO, List[FilePointer], List[StringIO]]
) -> Tuple[List[str], Iterator[Tuple]]:
    """
    Get the header and a list of row tuples from a buffer.

    If the arg is a list of buffers, we'll concatenate them ultimately, but
    because we don't know how big the buffers are, we can't just read them all
    into memory at once. So we'll read the first buffer, get the header and
    create a tuple iterator, and then read the remaining buffers and create
    tuple iterators for them. Then we'll concatenate the tuple iterators and
    return the header and the concatenated tuple iterator, without ever reading
    the entire buffer into memory.

    This also gives us the chance to check that the headers are the same for
    all buffers --- or optionally, not included in any of the buffers but the
    first one.

    """
    # If the input is a single buffer, make it a list of one buffer:
    if not isinstance(in_buffer, list):
        in_buffers = [in_buffer]
    else:
        in_buffers = in_buffer
    # Confirm all buffers are the same type (with .read() method) by opening
    # any file pointers:
    for i, buf in enumerate(in_buffers):
        if isinstance(buf, (str, pathlib.Path)):
            in_buffers[i] = open(buf, "r")

    # Read the first buffer and get the header:
    first_buffer = in_buffers[0]
    first_buffer.seek(0)
    header = _comma_separated_to_tuple(first_buffer.readline())
    # Create a tuple iterator for the first buffer, without the header:
    first_buffer_tuple_iterator = (
        tuple(line.strip().split(",")) for line in first_buffer.readlines()
    )
    # Create a tuple iterator for the remaining buffers, without the header:
    remaining_buffer_tuple_iterators = []
    for buf in in_buffers[1:]:
        buf.seek(0)
        # Get the first line. It MIGHT be a header, or it might be data. If
        # it's a header, we'll parity-check it against the first header. If
        # it's data, we'll prepend it to the data from the buffer.
        first_line = _comma_separated_to_tuple(buf.readline())
        if first_line == header:
            # It's a header, so we'll just skip it:
            #  TODO: Check for mismatched headers...
            # Put the rest of the buffer into the tuple iterator:
            remaining_buffer_tuple_iterators.append(
                (tuple(_comma_separated_to_tuple(line)) for line in buf.readlines())
            )
        else:
            # It's data, so we'll prepend it to the data from the buffer:
            remaining_buffer_tuple_iterators.append(
                itertools.chain(
                    [first_line],
                    (
                        tuple(
                            _comma_separated_to_tuple(line) for line in buf.readlines()
                        )
                    ),
                )
            )
    # Concatenate the tuple iterators:
    tuple_iterator = itertools.chain(
        first_buffer_tuple_iterator, *remaining_buffer_tuple_iterators
    )
    return header, tuple_iterator


def opencypher_buffers_to_graph(
    vertex_buffer: Union[FilePointer, StringIO, List[FilePointer], List[StringIO]],
    edge_buffer: Union[FilePointer, StringIO, List[FilePointer], List[StringIO]],
    to_graph: Union[nx.Graph, None] = None,
) -> nx.Graph:
    """
    Imports an OpenCypher-formatted pair of files/buffers into a graph that is
    compatible with the NetworkX API. If a graph is provided, the vertices and
    edges will be added to that graph. If no graph is provided, a new graph
    will be created.

    Arguments:
        vertex_buffer: A file path or buffer containing the vertex data.
        edge_buffer: A file path or buffer containing the edge data.
        to_graph: A NetworkX graph to which the vertices and edges will be
            added. If None, a new graph will be created.

    Returns:
        A NetworkX-like graph.

    """
    vert_header, vert_tuple_iterator = _get_nbuffer_header_and_tuple_iterator(
        vertex_buffer
    )
    edge_header, edge_tuple_iterator = _get_nbuffer_header_and_tuple_iterator(
        edge_buffer
    )

    return opencypher_iterators_to_graph(
        vert_tuple_iterator,
        edge_tuple_iterator,
        vertex_attributes=vert_header[2:],
        edge_attributes=edge_header[3:],
        to_graph=to_graph,
    )
