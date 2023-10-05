import io
import networkx as nx
from . import graph_to_opencypher_buffers, opencypher_buffers_to_graph


def test_one_vertex_graph():
    G = nx.Graph()
    G.add_node(0, name="Jordan")
    vb, eb = graph_to_opencypher_buffers(G)
    assert vb.read() == ":ID,:LABEL,name:String\n" + "0,Vertex,Jordan\n"
    assert eb.read() == ":START_ID,:END_ID,:TYPE\n"


def test_one_edge_graph():
    G = nx.Graph()
    G.add_edge("Jordan", "Bagels", type="likes")
    vb, eb = graph_to_opencypher_buffers(G)
    vbread = vb.read()
    ebread = eb.read()
    print(vbread)
    print(ebread)
    assert vbread == (":ID,:LABEL\n" "Jordan,Vertex\n" "Bagels,Vertex\n")
    assert (
        ebread == ":START_ID,:END_ID,:TYPE,type:String\n" "Jordan,Bagels,Edge,likes\n"
    )


def test_create_one_edge_graph():
    vert_str = (
        ":ID,:LABEL\n"
        "Jordan,Vertex\n"
        "Code,Vertex\n"
        "Coffee,Vertex\n"
        "Cortex,Vertex\n"
    )
    vert_buffer = io.StringIO(vert_str)
    edge_str = (
        ":START_ID,:END_ID,:TYPE,type:String\n"
        "Jordan,Code,Edge,likes\n"
        "Jordan,Coffee,Edge,likes\n"
        "Jordan,Cortex,Edge,likes\n"
    )
    edge_buffer = io.StringIO(edge_str)
    G = opencypher_buffers_to_graph(vert_buffer, edge_buffer)
    assert len(G.nodes) == 4
    assert len(G.edges) == 3


def test_create_graph_multi_vert_buffers():
    vert_str = (
        ":ID,:LABEL\n"
        "Jordan,Vertex\n"
        "Code,Vertex\n"
        "Coffee,Vertex\n"
        "Cortex,Vertex\n"
    )
    vert_buffer = io.StringIO(vert_str)
    vert_str2 = ":ID,:LABEL\n" "Fruitflies,Vertex\n" "Banana,Vertex\n"
    vert_buffer2 = io.StringIO(vert_str2)
    edge_str = (
        ":START_ID,:END_ID,:TYPE,type:String\n"
        "Jordan,Code,Edge,likes\n"
        "Jordan,Coffee,Edge,likes\n"
        "Jordan,Cortex,Edge,likes\n"
        "Fruitflies,Banana,Edge,likes\n"
    )
    edge_buffer = io.StringIO(edge_str)
    G = opencypher_buffers_to_graph([vert_buffer, vert_buffer2], edge_buffer)
    assert len(G.nodes) == 6
    assert len(G.edges) == 4


def test_create_graph_multi_edge_buffers():
    vert_str = (
        ":ID,:LABEL\n"
        "Jordan,Vertex\n"
        "Code,Vertex\n"
        "Coffee,Vertex\n"
        "Cortex,Vertex\n"
        "Fruitflies,Vertex\n"
        "Banana,Vertex\n"
        "Time,Vertex\n"
        "Arrow,Vertex\n"
    )
    vert_buffer = io.StringIO(vert_str)
    edge_str = (
        ":START_ID,:END_ID,:TYPE,type:String\n"
        "Jordan,Code,Edge,likes\n"
        "Jordan,Coffee,Edge,likes\n"
        "Jordan,Cortex,Edge,likes\n"
    )
    edge_buffer = io.StringIO(edge_str)
    edge_str2 = (
        ":START_ID,:END_ID,:TYPE,type:String\n"
        "Fruitflies,Banana,Edge,likes\n"
        "Time,Arrow,Edge,likes\n"
    )
    edge_buffer2 = io.StringIO(edge_str2)
    G = opencypher_buffers_to_graph(vert_buffer, [edge_buffer, edge_buffer2])
    assert len(G.nodes) == 8
    assert len(G.edges) == 5


def test_heterogenous_graph_combination():
    """
    Create two graphs with different node labels and combine them in one export
    """
    g1 = nx.DiGraph()
    g1.add_node(0, name="Jordan", type="Person")
    g1.add_node(1, name="Johns Hopkins", type="University")
    g1.add_edge(0, 1, type="affiliation")

    g2 = nx.DiGraph()
    g2.add_node(2, name="The Lobster", type="Movie")
    g2.add_node(3, name="Yorgos Lanthimos", type="Director")
    g2.add_edge(3, 2, type="directed")

    # Add `AffiliationGraph` label to g1 and `MovieGraph` label to g2
    for n in g1.nodes:
        g1.nodes[n]["__labels__"] = set(["AffiliationGraph", "PlainOlVertex"])
    for n in g2.nodes:
        g2.nodes[n]["__labels__"] = set(["MovieGraph", "PlainOlVertex"])

    # Combine the two graphs:
    G = nx.compose(g1, g2)

    vb, eb = graph_to_opencypher_buffers(G)
    vbread = vb.read()
    ebread = eb.read()
    print(vbread)
    print(ebread)
    assert vbread == (
        ":ID,:LABEL,name:String,type:String\n"
        "0,AffiliationGraph;PlainOlVertex,Jordan,Person\n"
        "1,AffiliationGraph;PlainOlVertex,Johns Hopkins,University\n"
        "2,MovieGraph;PlainOlVertex,The Lobster,Movie\n"
        "3,MovieGraph;PlainOlVertex,Yorgos Lanthimos,Director\n"
    )
