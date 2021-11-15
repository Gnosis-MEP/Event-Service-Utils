import unittest
from unittest.mock import patch


from .mocked_streams import MockedStreamFactory


class MockedServiceStreamTestCase(unittest.TestCase):
    GLOBAL_SERVICE_CONFIG = {
        'logging_level': 'DEBUG'
    }
    SERVICE_CLS = None
    MOCKED_STREAMS_DICT = {}

    def setUp(self):
        self.mocked_streams_dict = self.MOCKED_STREAMS_DICT.copy()
        self.service_cls = self.SERVICE_CLS
        self.service_config = self.GLOBAL_SERVICE_CONFIG
        self.prepare_mocked_stream_factory(self.mocked_streams_dict)
        self.service = self.instantiate_service()

    def prepare_mocked_stream_factory(self, mocked_dict):
        self.stream_factory = MockedStreamFactory(mocked_dict=self.mocked_streams_dict)

    def tearDown(self):
        pass

    def instantiate_service(self):
        service_kwargs = self.service_config.copy()
        # quickfix by piyush for testcase of window-manager
        if 'no_stream_factory_flag' not in service_kwargs:
            service_kwargs.update({'stream_factory': self.stream_factory})
        else:
            del service_kwargs['no_stream_factory_flag']
        with patch('event_service_utils.tracing.jaeger.init_tracer') as mockedTracer:
            self.service = self.service_cls(**service_kwargs)
            if hasattr(self.service, 'tracer') and self.service.tracer:
                self.service.tracer.close()
            self.service.tracer = mockedTracer
        return self.service


class MockedEventDrivenServiceStreamTestCase(MockedServiceStreamTestCase):
    GLOBAL_SERVICE_CONFIG = {
        'logging_level': 'DEBUG'
    }
    SERVICE_CLS = None
    MOCKED_CG_STREAM_DICT = {
    }
    MOCKED_STREAMS_DICT = {}

    def instantiate_service(self):
        service_kwargs = self.service_config.copy()
        service_kwargs.update({'stream_factory': self.stream_factory})
        with patch('event_service_utils.tracing.jaeger.init_tracer') as mockedTracer:
            self.service = self.service_cls(**service_kwargs)
            if hasattr(self.service, 'tracer') and self.service.tracer:
                self.service.tracer.close()
            self.service.tracer = mockedTracer
        return self.service
