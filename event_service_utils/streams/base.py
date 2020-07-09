
class BasicStream():
    """
    Base stream class
    """

    def __init__(self, key):
        self.key = key

    def read_events(self, count=1):
        raise NotImplementedError()

    def write_events(self, *events):
        raise NotImplementedError()

    def ack(self, event_id, stream_key=None):
        raise NotImplementedError()


class StreamFactory():
    def __init__(self):
        pass

    def create(self, key, stype=None):
        raise NotImplementedError()
