import uuid
import json
import logging
import logzero


class BaseService():
    def __init__(self, name, service_stream_key, service_cmd_key, stream_factory, logging_level):
        self.name = name
        self.logging_level = logging_level
        self.stream_factory = stream_factory
        self.service_stream = self.stream_factory.create(service_stream_key)
        self.service_cmd = self.stream_factory.create(service_cmd_key, stype='streamOnly')
        self.logger = self._setup_logging()
        self.cmd_validation_fields = ['id', 'action']
        self.data_validation_fields = ['id']
        self.ack_data_stream_events = True

    def _setup_logging(self):
        log_format = (
            '%(color)s[%(levelname)1.1s %(name)s %(asctime)s:%(msecs)d '
            '%(module)s:%(funcName)s:%(lineno)d]%(end_color)s %(message)s'
        )
        formatter = logzero.LogFormatter(fmt=log_format)
        return logzero.setup_logger(name=self.name, level=logging.getLevelName(self.logging_level), formatter=formatter)

    def log_state(self):
        self.logger.debug('Current State:')

    def _log_dict(self, dict_name, dict):
        log_msg = f'- {dict_name}:'
        for k, v in dict.items():
            log_msg += f'\n-- {k}  ---  {v}'
        self.logger.debug(log_msg)

    def default_event_serializer(self, event_data):
        event_msg = {'event': json.dumps(event_data)}
        return event_msg

    def default_event_deserializer(self, json_msg):
        event_key = b'event' if b'event' in json_msg else 'event'
        event_json = json_msg.get(event_key, '{}')
        event_data = json.loads(event_json)
        return event_data

    def service_based_random_event_id(self):
        return f'{self.name}:{str(uuid.uuid4())}'

    def event_validation_fields(self, event_data, fields):
        missing_fields = []
        for field in fields:
            if field not in event_data:
                missing_fields.append(field)

        if missing_fields:
            self.logger.error(f'Missing fields: "{missing_fields}" in event "{event_data}".')
            return False
        return True

    def process_data_event(self, event_data, json_msg):
        if not self.event_validation_fields(event_data, self.data_validation_fields):
            self.logger.info(f'Ignoring bad event data: {event_data}')
            return False
        self.logger.debug(f'Processing new data event: {event_data}')
        return True

    def process_data_event_wrapper(self, event_data, json_msg):
        self.process_data_event(event_data, json_msg)

    def process_data(self):
        self.logger.debug('Processing DATA..')
        if not self.service_stream:
            return
        event_list = self.service_stream.read_events(count=1)
        for event_tuple in event_list:
            event_id, json_msg = event_tuple
            try:
                event_data = self.default_event_deserializer(json_msg)
                self.process_data_event_wrapper(event_data, json_msg)
            except Exception as e:
                self.logger.error(f'Error processing {json_msg}:')
                self.logger.exception(e)
            finally:
                if self.ack_data_stream_events:
                    # we are always ack the events, even if they fail.
                    # in a better world we would actually do some treatments to
                    # see if the event should be re-processed or not, before ack.
                    self.service_stream.ack(event_id)

    def process_action(self, action, event_data, json_msg):
        if not self.event_validation_fields(event_data, self.cmd_validation_fields):
            self.logger.info(f'Ignoring bad event data: {event_data}')
            return False
        self.logger.debug('processing action: "%s" with this args: "%s"' % (event_data['action'], event_data))
        return True

    def process_action_wrapper(self, event_data, json_msg):
        action = event_data.get('action')
        self.process_action(action, event_data, json_msg)

    def process_cmd(self):
        self.logger.debug('Processing CMD..')

        event_list = self.service_cmd.read_events(count=1)
        for event_tuple in event_list:
            event_id, json_msg = event_tuple
            try:
                event_data = self.default_event_deserializer(json_msg)
                self.process_action_wrapper(event_data, json_msg)
                self.log_state()
            except Exception as e:
                self.logger.error(f'Error processing {json_msg}:')
                self.logger.exception(e)

    def run_forever(self, method, **kwargs):
        while True:
            method(**kwargs)

    def run(self):
        self.logger.info(f'starting {self.name}')
