import networkx as nx

from networkx.generators.classic import complete_graph


def load_graph_nodes_from_tuples(graph, node_tuples):
    for node in node_tuples:
        node_id, attributes = node
        graph.add_node(node_id, **attributes)
    return graph


def load_graph_edges_from_tuples(graph, edge_tuples):
    for edge in edge_tuples:
        node_u, node_v, attributes = edge
        graph.add_edge(node_u, node_v, **attributes)
    return graph


def ensure_is_complete_graph(node_ids):
    graph = complete_graph(n=node_ids)
    return graph


def load_graph_from_tuples_dict(graph_tuples_dict):
    graph = nx.Graph()
    node_tuples = graph_tuples_dict.get('nodes', ())
    node_ids = [node[0] for node in node_tuples]
    graph = ensure_is_complete_graph(node_ids)
    graph = load_graph_nodes_from_tuples(graph, node_tuples)

    edge_tuples = graph_tuples_dict.get('edges', ())
    graph = load_graph_edges_from_tuples(graph, edge_tuples)

    return graph
