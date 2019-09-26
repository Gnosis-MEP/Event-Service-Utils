import logging
from jaeger_client import Config


def init_tracer(service_name, reporting_host, reporting_port):
    logging.getLogger('').handlers = []
    logging.basicConfig(format='%(message)s', level=logging.DEBUG)

    config = Config(
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

    return config.initialize_tracer()
