from .base import VEKG
from redisgraph import Node, Edge, Graph


class RedisGraph(VEKG):

    def __init__(self, graph, fetch_type='lazy', **kwargs):
        super().__init__(graph)
        if 'eager' == fetch_type:
            self.retrieve_all_nodes_and_edges()
        if 'eager_matched_nodes' == fetch_type:
            self.retrieve_all_matched_nodes_and_edges()

    def add_node(self, node_id, label, properties=None):
        if properties is None:
            properties = {}
        node = Node(node_id=node_id, label=label, properties=properties)
        self.graph.add_node(node)

    def add_edge(self, src_node, dest_node, relation=None, properties=None):
        if properties is None:
            properties = {}
        if relation is None:
            relation = ''
        edge = Edge(src_node=src_node, relation=relation, dest_node=dest_node, properties=properties)
        self.graph.add_edge(edge)

    @property
    def nodes(self):
        return self.graph.nodes

    @property
    def edges(self):
        return self.graph.edges

    def retrieve_all_nodes_and_edges(self):
        node_list = self.execute_query('MATCH (n) RETURN n')
        for nodes in node_list:
            for node in nodes:
                self.graph.add_node(node)

        edge_list = self.execute_query('MATCH (n)-[r]->(m) RETURN n,r,m')
        for edge_pair in edge_list:
            edge = edge_pair[1]
            self.graph.edges.append(Edge(edge_pair[0], edge.relation, edge_pair[2], edge.id, edge.properties))

    def retrieve_all_matched_nodes_and_edges(self):
        node_list = self.execute_query("MATCH (n) WHERE n.is_matched=true RETURN n")
        for nodes in node_list:
            for node in nodes:
                self.graph.add_node(node)

        edge_list = self.execute_query(
            "MATCH (n)-[r]->(m) WHERE n.is_matched=true AND m.is_matched=true RETURN n,r,m")
        for edge_pair in edge_list:
            edge = edge_pair[1]
            self.graph.edges.append(Edge(edge_pair[0], edge.relation, edge_pair[2], edge.id, edge.properties))

    def execute_query(self, query, params=None):
        if params is None:
            params = dict()
        result = self.graph.query(query, params=params)
        # result.pretty_print()
        return result.result_set

    def execute_query_for_output(self, query, params=None):
        if params is None:
            params = dict()
        result = self.graph.query(query, params=params)
        return result.header, result.result_set

    def commit(self):
        self.graph.flush()

    def drop(self):
        self.graph.delete()


class RedisGraphEngine():

    def __init__(self, redis_conn):
        self.redis_conn = redis_conn

    def get_graph_instance(self, graph_id, fetch_type="lazy", **kwargs):
        redis_graph = Graph(graph_id, self.redis_conn)
        return RedisGraph(redis_graph, fetch_type, **kwargs)
