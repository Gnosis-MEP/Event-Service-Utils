from walrus import Database

from .base import BasicStream, StreamFactory


class RedisStreamAndConsumer(BasicStream):
    def __init__(self, redis_db, key, max_stream_length=10):
        BasicStream.__init__(self, key)
        self.redis_db = redis_db
        self.output_stream = self._get_stream(key)
        self.input_consumer_group = self._get_single_stream_consumer_group(key)
        self.max_stream_length = max_stream_length

    def _get_single_stream_consumer_group(self, key):
        group_name = 'cg-%s' % key
        consumer_group = self.redis_db.consumer_group(group_name, key)
        consumer_group.create()
        consumer_group.set_id(id='$')
        return consumer_group

    def read_events(self, count=1):
        streams_events_list = self.input_consumer_group.read(count=count)
        for stream_id, event_list in streams_events_list:
            yield from event_list

    def _get_stream(self, key):
        return self.redis_db.Stream(key)

    def write_events(self, *events):
        return [
            self.output_stream.add(data=event, maxlen=self.max_stream_length, approximate=False) for event in events
        ]


class RedisStreamOnly(BasicStream):
    def __init__(self, redis_db, key, max_stream_length=10):
        BasicStream.__init__(self, key)
        self.redis_db = redis_db
        self.single_io_stream = self._get_stream(key)
        self.single_io_stream.read(count=10)
        self.last_msg_id = None
        if self.single_io_stream.length() != 0:
            self.last_msg_id = self.single_io_stream.info()['last-entry'][0]

        self.max_stream_length = max_stream_length

    def read_events(self, count=1):
        events_list = self.single_io_stream.read(count=count, last_id=self.last_msg_id)
        if events_list:
            self.last_msg_id = events_list[-1][0]
        yield from events_list

    def _get_stream(self, key):
        return self.redis_db.Stream(key)

    def write_events(self, *events):
        return [
            self.single_io_stream.add(data=event, maxlen=self.max_stream_length, approximate=False) for event in events
        ]


class RedisStreamFactory(StreamFactory):

    def __init__(self, host='localhost', port='6379', max_stream_length=10):
        self.redis_db = Database(host=host, port=port)
        self.max_stream_length = max_stream_length

    def create(self, key, stype='streamAndConsumer'):
        if stype == 'streamAndConsumer':
            return RedisStreamAndConsumer(redis_db=self.redis_db, key=key, max_stream_length=self.max_stream_length)
        elif stype == 'streamOnly':
            return RedisStreamOnly(redis_db=self.redis_db, key=key, max_stream_length=self.max_stream_length)