from opentracing.ext import tags
from opentracing.propagation import Format
from prometheus_client import REGISTRY
from prometheus_client import start_http_server

from .base import BaseService

EVENT_ID_TAG = 'event-id'
ACTION_NAME_TAG = 'process-action-name'


class BaseTracerService(BaseService):
    def __init__(
            self, name, service_stream_key, service_cmd_key, stream_factory, logging_level, tracer):
        super(BaseTracerService, self).__init__(
            name=name,
            service_stream_key=service_stream_key,
            service_cmd_key=service_cmd_key,
            stream_factory=stream_factory,
            logging_level=logging_level
        )
        self.tracer = tracer
        self.prometheus_metrics_on_traces = False
        self.start_prometheus_http_server = False

    def get_event_tracer_kwargs(self, event_data):
        tracer_kwargs = {}
        tracer_data = event_data.get('tracer', {})
        tracer_headers = tracer_data.get('headers', {})
        if tracer_headers:
            span_ctx = self.tracer.extract(Format.HTTP_HEADERS, tracer_headers)
            tracer_kwargs.update({
                'child_of': span_ctx
            })
        else:
            self.logger.info(f'No tracer id found on event id: {event_data["id"]}')
            self.logger.info(
                (
                    'Will start a new tracer id.'
                    'If this event came from another service '
                    'this will likelly cause confusion in the current event tracing')
            )
        return tracer_kwargs

    def event_trace_for_method_with_event_data(
            self, method, method_args, method_kwargs, get_event_tracer=False, tracer_tags=None):
        if hasattr(self, 'prometheus_metrics_on_traces') and self.prometheus_metrics_on_traces:
            for metric in REGISTRY.collect():
                for sample in metric.samples:
                    tag_name = f'{sample.name}{sample.labels}'
                    tracer_tags[tag_name] = REGISTRY.get_sample_value(sample.name, sample.labels)

        span_name = method.__name__
        if tracer_tags is None:
            tracer_tags = {}

        tracer_kwargs = {}
        if get_event_tracer:
            event_data = method_kwargs['event_data']
            tracer_kwargs = self.get_event_tracer_kwargs(event_data)
        with self.tracer.start_active_span(span_name, **tracer_kwargs) as scope:
            for tag, value in tracer_tags.items():
                scope.span.set_tag(tag, value)
            method(*method_args, **method_kwargs)

    def inject_current_tracer_into_event_data(self, event_data):
        tracer_data = event_data.setdefault('tracer', {})
        tracer_headers = tracer_data.setdefault('headers', {})
        # span = self.tracer.active_span
        # self.tracer.inject(span, Format.HTTP_HEADERS, tracer_headers)
        with self.tracer.start_active_span('tracer_injection') as scope:
            scope.span.set_tag(EVENT_ID_TAG, event_data['id'])
            self.tracer.inject(scope.span, Format.HTTP_HEADERS, tracer_headers)
        return event_data

    def serialize_and_write_event_with_trace(self, event_data, serializer, destination_stream):
        event_data = self.inject_current_tracer_into_event_data(event_data)
        event_msg = serializer(event_data)
        return destination_stream.write_events(event_msg)

    def write_event_with_trace(self, event_data, destination_stream, serializer=None):
        if serializer is None:
            serializer = self.default_event_serializer
        self.event_trace_for_method_with_event_data(
            method=self.serialize_and_write_event_with_trace,
            method_args=(),
            method_kwargs={
                'event_data': event_data,
                'serializer': serializer,
                'destination_stream': destination_stream
            },
            get_event_tracer=False,
            tracer_tags={
                tags.MESSAGE_BUS_DESTINATION: destination_stream.key,
                tags.SPAN_KIND: tags.SPAN_KIND_PRODUCER,
                EVENT_ID_TAG: event_data['id'],
            }
        )

    def process_action_wrapper(self, event_data, json_msg):
        action = event_data.get('action', '')
        self.event_trace_for_method_with_event_data(
            method=self.process_action,
            method_args=(),
            method_kwargs={
                'action': action,
                'event_data': event_data,
                'json_msg': json_msg
            },
            get_event_tracer=True,
            tracer_tags={
                tags.SPAN_KIND: tags.SPAN_KIND_CONSUMER,
                EVENT_ID_TAG: event_data['id'],
                ACTION_NAME_TAG: action,
            }
        )

    def process_data_event_wrapper(self, event_data, json_msg):
        self.event_trace_for_method_with_event_data(
            method=self.process_data_event,
            method_args=(),
            method_kwargs={
                'event_data': event_data,
                'json_msg': json_msg
            },
            get_event_tracer=True,
            tracer_tags={
                tags.SPAN_KIND: tags.SPAN_KIND_CONSUMER,
                EVENT_ID_TAG: event_data['id'],
            }
        )

    def run(self):
        super(BaseTracerService, self).run()
        if hasattr(self, 'start_prometheus_http_server') and self.start_prometheus_http_server:
            start_http_server(8000)
