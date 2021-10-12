from .tracer import BaseTracerService


class BaseRegistryService(BaseTracerService):
    def __init__(
            self, name, service_stream_key, service_cmd_key, service_registry_cmd_key, service_details, stream_factory,
            logging_level, tracer):
        super(BaseRegistryService, self).__init__(
            name=name,
            service_stream_key=service_stream_key,
            service_cmd_key=service_cmd_key,
            stream_factory=stream_factory,
            logging_level=logging_level,
            tracer=tracer
        )
        self.service_details = service_details
        self.service_registry_cmd = self.stream_factory.create(service_registry_cmd_key, stype='streamOnly')

    def annouce_server(self):
        new_event_data = {
            'id': self.service_based_random_event_id(),
            'action': 'addServiceWorker',
            'worker': self.service_details
        }
        self.write_event_with_trace(new_event_data, self.service_registry_cmd)

    def run(self):
        super(BaseRegistryService, self).run()
        if self.service_details is not None:
            self.annouce_server()
