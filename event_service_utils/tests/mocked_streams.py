from event_service_utils.streams.base import BasicStream, StreamFactory


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

    def ack_events(self, *message_ids):
        pass


class MockedManyKeyConsumerGroupOnly(BasicStream):
    def __init__(self, keys, mocked_values_dict, cg_id=None):
        BasicStream.__init__(self, cg_id)
        # mocked event list dict
        self.mocked_values_dict = mocked_values_dict

    def read_stream_events_list(self, count=1):
        next_event_list = []
        for stream, mocked_values in self.mocked_values_dict.items():
            stream_event_list = []
            for i in range(count):
                stream_event = self.mocked_values_dict[stream].pop(0)
                if stream_event:
                    stream_event_list.append(stream_event)
            next_event_list.append([stream, stream_event_list])
        return next_event_list


class MockedStreamFactory(StreamFactory):

    def __init__(self, mocked_dict):
        self.mocked_dict = mocked_dict

    def create(self, key, stype=None, cg_id=None):
        if stype == 'manyKeyConsumerOnly':
            return MockedManyKeyConsumerGroupOnly(keys=key, mocked_values_dict=self.mocked_dict[cg_id], cg_id=cg_id)
        if key not in self.mocked_dict:
            self.mocked_dict[key] = []
        return MockedStreamAndConsumer(key=key, mocked_values=self.mocked_dict[key])


