import logging
from jaeger_client import Config
from jaeger_client.metrics.prometheus import PrometheusMetricsFactory


def init_tracer(service_name, reporting_host, reporting_port, use_metrics=False, logging_level=logging.ERROR):
    logging.getLogger('').handlers = []
    logging.basicConfig(format='%(message)s', level=logging_level)
    config_kwargs = {}
    config_kwargs.update(
        config={  # usually read from some yaml config
            'sampler': {
                'type': 'const',
                'param': 1,
            },
            'local_agent': {
                'reporting_host': reporting_host,
                'reporting_port': reporting_port,
            },
            'logging': True,
            'reporter_batch_size': 1,
        },
        service_name=service_name,
    )
    if use_metrics:
        config_kwargs.update(metrics_factory=PrometheusMetricsFactory(namespace=service_name))
    config = Config(**config_kwargs)

    return config.initialize_tracer()
