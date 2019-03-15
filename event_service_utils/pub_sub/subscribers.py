from .base import PubSubAnnounceActionsMixin


class Subscriber(PubSubAnnounceActionsMixin):
    def __init__(self, user_id, subscription, stream_factory, user_manager_stream_key, event_processor):
        super(Subscriber, self).__init__(user_id, 'subscribe', 'unsubscribe', stream_factory, user_manager_stream_key)
        self.subscription = subscription
        self.event_processor = event_processor

    def start(self):
        print(f"'{self.user_id}' Subscribing to '{self.subscription}' with '{self.event_processor}' event_processor")
        super(Subscriber, self).start()
        try:
            while True:
                for event_tuple in self.stream.read_events(count=1):
                    self.event_processor.process(event_tuple)
        finally:
            self.stop()
