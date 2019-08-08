import unittest

from event_service_utils.schemas.internal_msgs import (
    BaseInternalMessage
)

from .mocked_streams import MockedStreamFactory


class MockedServiceStreamTestCase(unittest.TestCase):
    GLOBAL_SERVICE_CONFIG = {}
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
        service_kwargs.update({'stream_factory': self.stream_factory})
        self.service = self.service_cls(**service_kwargs)
        return self.service
        # self.service.run()
