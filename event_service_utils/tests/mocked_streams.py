from event_service_utils.base import BasicStream, StreamFactory


class MockedStreamAndConsumer(BasicStream):
    def __init__(self, key, mocked_values):
        BasicStream.__init__(self, key)
        # mocked event list
        self.mocked_values = mocked_values

    def read_events(self, count=1):
        event_list = []
        for i in range(count):
            if self.mocked_values:
                next_event = self.mocked_values.pop(0)
                if next_event:
                    event_list.append(next_event)
                    yield next_event
            else:
                return None

    def write_events(self, *events):
        self.mocked_values.extend(events)
        return self.mocked_values


class MockedStreamFactory(StreamFactory):

    def __init__(self, mocked_dict):
        self.mocked_dict = mocked_dict

    def create(self, key, stype=None):
        return MockedStreamAndConsumer(key=key, mocked_values=self.mocked_dict[key])
