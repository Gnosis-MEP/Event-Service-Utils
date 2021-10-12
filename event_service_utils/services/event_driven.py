from .tracer import EVENT_ID_TAG, tags
from .registry import BaseRegistryService

EVENT_TYPE_TAG = 'event-type'
JSON_MSG_TAG = 'json-msg'


class BaseEventDrivenCMDService(BaseRegistryService):
    def __init__(self, name, service_stream_key, service_cmd_key_list, service_registry_cmd_key,
                 service_details, stream_factory, logging_level, tracer):
        self.name = name
        self.logging_level = logging_level
        self.stream_factory = stream_factory
        self.service_stream = self.stream_factory.create(service_stream_key)
        self.service_cmd_key_list = service_cmd_key_list
        self.service_cmd = self.stream_factory.create(service_cmd_key_list, stype='manyKeyConsumerOnly')
        self.logger = self._setup_logging()
        self.cmd_validation_fields = ['id', 'action']
        self.data_validation_fields = ['id']
        self.ack_data_stream_events = True
        self.tracer = tracer
        self.service_details = service_details
        self.service_registry_cmd = self.stream_factory.create(service_registry_cmd_key, stype='streamOnly')

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
        self.logger.debug(f'Processing CMD from event types: {self.service_cmd_key_list}..')

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
