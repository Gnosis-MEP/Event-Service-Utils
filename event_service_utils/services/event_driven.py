import re

from .tracer import EVENT_ID_TAG, tags
from .registry import BaseTracerService

EVENT_TYPE_TAG = 'event-type'
JSON_MSG_TAG = 'json-msg'


class BaseEventDrivenCMDService(BaseTracerService):
    def __init__(self, name, service_stream_key, service_cmd_key_list, pub_event_list,
                 service_details, stream_factory, logging_level, tracer):
        self.name = name
        self.logging_level = logging_level
        self.stream_factory = stream_factory
        self.service_stream = self.stream_factory.create(service_stream_key)
        self.service_cmd_key_list = service_cmd_key_list
        self.service_cmd = self.stream_factory.create(
            service_cmd_key_list, stype='manyKeyConsumerOnly', cg_id=f'cg-{self.name}')
        self.logger = self._setup_logging()
        self.cmd_validation_fields = ['id']
        self.data_validation_fields = ['id']
        self.ack_data_stream_events = True
        self.tracer = tracer
        self.service_details = service_details
        self.pub_event_list = pub_event_list
        self.pub_event_stream_map = {}
        self.init_publishing_event_stream()

    def init_publishing_event_stream(self):
        if self.service_details is not None:
            anounce_service = 'ServiceWorkerAnnounced'
            assert anounce_service in self.pub_event_list, f'"{anounce_service}" should be present'
        for event_type in self.pub_event_list:
            stream = self.stream_factory.create(event_type, stype='streamOnly')
            event_type_slugfy = re.sub(r'(?<!^)(?=[A-Z])', '_', event_type).lower()
            attr_name = f'pub_stream_{event_type_slugfy}'
            setattr(self, attr_name, stream)
            self.pub_event_stream_map[event_type] = stream

    def publish_event_type_to_stream(self, event_type, new_event_data):
        pub_stream = self.pub_event_stream_map.get(event_type)
        if pub_stream is None:
            raise RuntimeError(f'No publishing stream defined for event type: {event_type}!')

        self.logger.info(f'Publishing "{event_type}" entity: {new_event_data}')
        self.write_event_with_trace(new_event_data, pub_stream)

    def process_event_type(self, event_type, event_data, json_msg):
        if not self.event_validation_fields(event_data, self.cmd_validation_fields):
            self.logger.info(f'Ignoring bad event data: {event_data}')
            return False
        self.logger.debug(f'processing event type: "{event_type}" with this args: "{event_data}"')
        return True

    def process_event_type_wrapper(self, event_type, event_data, json_msg):
        tracer_tags = {
            tags.SPAN_KIND: tags.SPAN_KIND_CONSUMER,
            EVENT_ID_TAG: event_data['id'],
            EVENT_TYPE_TAG: event_type
        }
        if self.logging_level == 'DEBUG':
            tracer_tags[JSON_MSG_TAG] = json_msg

        self.event_trace_for_method_with_event_data(
            method=self.process_event_type,
            method_args=(),
            method_kwargs={
                'event_type': event_type,
                'event_data': event_data,
                'json_msg': json_msg
            },
            get_event_tracer=True,
            tracer_tags=tracer_tags
        )

    def process_cmd(self):
        self.logger.debug(f'Processing CMD from event types: {self.service_cmd_key_list}')

        stream_event_list = self.service_cmd.read_stream_events_list(count=1)
        for stream_key, event_tuple in stream_event_list:
            event_type = stream_key.decode('utf-8')
            event_id, json_msg = event_tuple[0]
            try:
                event_data = self.default_event_deserializer(json_msg)
                self.process_event_type_wrapper(event_type, event_data, json_msg)
                self.log_state()
            except Exception as e:
                self.logger.error(f'Error processing {json_msg}:')
                self.logger.exception(e)

    def annouce_service_worker(self):
        new_event_data = {
            'id': self.service_based_random_event_id(),
            'worker': self.service_details
        }
        self.write_event_with_trace(new_event_data, self.pub_stream_service_worker_announced)

    def run(self):
        super(BaseEventDrivenCMDService, self).run()
        if self.service_details is not None:
            self.annouce_service_worker()
