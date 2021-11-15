from walrus import Database
from walrus.containers import make_python_attr as walrus_normalized_cg_stream_key

from .base import BasicStream, StreamFactory


class RedisStreamAndConsumer(BasicStream):
    def __init__(self, redis_db, key, max_stream_length=None, block=0, create_cg=True):
        BasicStream.__init__(self, key)
        self.block = block
        self.redis_db = redis_db
        self.create_cg = create_cg
        self.output_stream = self._get_stream(key)
        self.input_consumer_group = self._get_single_stream_consumer_group(key)
        self.max_stream_length = max_stream_length
        self._set_default_write_kwargs()

    def _set_default_write_kwargs(self):
        write_kwargs = {
        }
        if self.max_stream_length is not None:
            write_kwargs.update({
                'maxlen': self.max_stream_length,
                'approximate': False
            })
        self.default_write_kwargs = write_kwargs

    def _get_single_stream_consumer_group(self, key):
        group_name = 'cg-%s' % key
        consumer_group = self.redis_db.consumer_group(group_name, key)
        if self.create_cg:
            consumer_group.create()
        consumer_group.set_id(id='$')
        return consumer_group

    def read_events(self, count=1):
        streams_events_list = self.input_consumer_group.read(count=count, block=self.block)
        for stream_id, event_list in streams_events_list:
            yield from event_list

    def _get_stream(self, key):
        return self.redis_db.Stream(key)

    def write_events(self, *events):
        return [
            self.output_stream.add(data=event, **self.default_write_kwargs) for event in events
        ]

    def ack(self, event_id, stream_key=None):
        if stream_key is None:
            stream_key = self.key
        cg_stream = getattr(self.input_consumer_group, walrus_normalized_cg_stream_key(stream_key))
        return cg_stream.ack(event_id)


class ManyKeyConsumerGroupOnly(BasicStream):
    def __init__(self, redis_db, keys, max_stream_length=None, block=0, create_cg=True, cg_id=None):
        if cg_id is None:
            self.cg_id = 'cg-' + '-'.join(keys)
        self.cg_id = cg_id
        BasicStream.__init__(self, self.cg_id)
        self.block = block
        self.redis_db = redis_db
        self.input_consumer_group = self._get_many_stream_consumer_group(keys)
        self.max_stream_length = max_stream_length

    def _get_many_stream_consumer_group(self, keys):
        group_name = self.cg_id
        consumer_group = self.redis_db.consumer_group(group_name, keys)
        consumer_group.create()
        consumer_group.set_id(id='$')
        return consumer_group

    def read_stream_events_list(self, count=1):
        return self.input_consumer_group.read(count=count, block=self.block)


class RedisStreamOnly(BasicStream):
    def __init__(self, redis_db, key, max_stream_length=None, block=0):
        BasicStream.__init__(self, key)
        self.block = block
        self.redis_db = redis_db
        self.single_io_stream = self._get_stream(key)
        self.single_io_stream.read(count=10)
        self.last_msg_id = None
        if self.single_io_stream.length() != 0:
            self.last_msg_id = self.single_io_stream.info()['last-entry'][0]

        self.max_stream_length = max_stream_length
        self._set_default_write_kwargs()

    def _set_default_write_kwargs(self):
        write_kwargs = {
        }
        if self.max_stream_length is not None:
            write_kwargs.update({
                'maxlen': self.max_stream_length,
                'approximate': False
            })
        self.default_write_kwargs = write_kwargs

    def read_events(self, count=1):
        events_list = self.single_io_stream.read(count=count, last_id=self.last_msg_id, block=self.block)
        if events_list:
            self.last_msg_id = events_list[-1][0]
        yield from events_list

    def _get_stream(self, key):
        return self.redis_db.Stream(key)

    def write_events(self, *events):
        return [
            self.single_io_stream.add(data=event, **self.default_write_kwargs) for event in events
        ]


class RedisStreamFactory(StreamFactory):

    def __init__(self, host='localhost', port='6379', max_stream_length=None, block=0):
        self.block = block
        self.redis_db = Database(host=host, port=port)
        self.max_stream_length = max_stream_length

    def create(self, key, stype='streamAndConsumer', cg_id=None):
        if stype == 'streamAndConsumer':
            return RedisStreamAndConsumer(
                redis_db=self.redis_db, key=key, max_stream_length=self.max_stream_length, block=self.block)
        elif stype == 'streamOnly':
            return RedisStreamOnly(
                redis_db=self.redis_db, key=key, max_stream_length=self.max_stream_length, block=self.block)
        elif stype == 'manyKeyConsumerOnly':
            return ManyKeyConsumerGroupOnly(
                redis_db=self.redis_db, keys=key,
                max_stream_length=self.max_stream_length, block=self.block,
                cg_id=cg_id
            )
