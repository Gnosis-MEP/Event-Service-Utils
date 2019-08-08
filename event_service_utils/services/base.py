import logging
import logzero


class BaseService():
    def __init__(self, name, service_stream_key, service_cmd_key, cmd_event_schema, stream_factory, logging_level):
        self.name = name
        self.logging_level = logging_level
        self.stream_factory = stream_factory
        self.cmd_event_schema = cmd_event_schema
        self.service_stream = self.stream_factory.create(service_stream_key)
        self.service_cmd = self.stream_factory.create(service_cmd_key, stype='streamOnly')
        self.logger = self._setup_logging()

    def _setup_logging(self):
        log_format = (
            '%(color)s[%(levelname)1.1s %(name)s %(asctime)s:%(msecs)d '
            '%(module)s:%(funcName)s:%(lineno)d]%(end_color)s %(message)s'
        )
        formatter = logzero.LogFormatter(fmt=log_format)
        return logzero.setup_logger(name=self.name, level=logging.getLevelName(self.logging_level), formatter=formatter)

    def process_action(self, action, event_data, json_msg):
        self.logger.debug('processing action: "%s" with this args: "%s"' % (event_data['action'], event_data))

    def log_state(self):
        self.logger.debug('Current State:')

    def _log_dict(self, dict_name, dict):
        log_msg = f'- {dict_name}:'
        for k, v in dict.items():
            log_msg += f'\n-- {k}  ---  {v}'
        self.logger.debug(log_msg)

    def process_cmd(self):
        self.logger.debug('Processing CMD..')
        event_list = self.service_cmd.read_events(count=1)
        for event_tuple in event_list:
            event_id, json_msg = event_tuple
            event_schema = self.cmd_event_schema(json_msg=json_msg)
            event_data = event_schema.object_load_from_msg()
            action = event_data['action']
            self.process_action(action, event_data, json_msg)
            self.log_state()

    def run_forever(self, method):
        while True:
            method()

    def run(self):
        self.logger.info(f'starting {self.name}')
