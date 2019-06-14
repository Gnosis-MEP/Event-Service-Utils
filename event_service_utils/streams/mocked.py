from .base import BasicStream, StreamFactory


class MockedStreamAndConsumer(BasicStream):
    def __init__(self, key, mocked_values):
        BasicStream.__init__(self, key)
        # mocked event list
        self.mocked_values = mocked_values

    def read_events(self, count=1):
        yield from self.mocked_values

    def write_events(self, *events):
        self.mocked_values.extend(events)
        return self.mocked_values


class MockedStreamFactory(StreamFactory):

    def __init__(self, mocked_dict):
        self.mocked_dict = mocked_dict

    def create(self, key, stype=None):
        return MockedStreamAndConsumer(key=key, mocked_values=self.mocked_dict[key])