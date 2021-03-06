from redisgraph import Node, Edge


class MockedGraph():

    def __init__(self, graph_id, fetch_type):
        super().__init__()
        self.graph = {'id': graph_id, 'nodes':[], 'edges':[]}
        if 'eager' == fetch_type:
            self.retrieve_all_nodes_and_edges()

    def add_node(self, node_id, label, properties=None):
        if properties is None:
            properties = {}
        node = Node(node_id=node_id, label=label, properties=properties)
        self.graph['nodes'].append(node)

    def add_edge(self, src_node, dest_node, relation=None, properties=None):
        if properties is None:
            properties = {}
        if relation is None:
            relation = ''
        edge = Edge(src_node=src_node, relation=relation, dest_node=dest_node, properties=properties)
        self.graph['edges'].append(edge)

    def nodes(self):
        return self.graph['nodes']

    def edges(self):
        return self.graph['edges']

    def retrieve_all_nodes_and_edges(self):
        pass

    def execute_query(self, query, params=None):
        pass


class MockedGraphEngine():

    def __init__(self, redis_conn):
        self.redis_conn = redis_conn

    def get_graph_instance(self, graph_id, fetch_type="lazy"):
        redis_graph = MockedGraph(graph_id, self.redis_conn)
        return MockedGraph(redis_graph, fetch_type)


class MockedGraphFactory():

    def __init__(self, mocked_dict):
        super().__init__()
        self.mocked_dict = mocked_dict

    def create(self, engine_type):
        if engine_type == 'redis_graph':
            return MockedGraphEngine(self.mocked_dict)