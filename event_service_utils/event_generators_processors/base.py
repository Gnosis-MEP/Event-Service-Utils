class BaseEventGenerator():
    def __init__(self, source, event_schema):
        self.source = source
        self.event_schema = event_schema

    def next_event(self):
        raise NotImplementedError()


class BaseEventProcessor():
    def __init__(self, event_schema):
        self.event_schema = event_schema

    def process(self, event_tuple):
        raise NotImplementedError()
