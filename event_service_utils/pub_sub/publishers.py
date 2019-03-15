from .base import PubSubAnnounceActionsMixin


class Publisher(PubSubAnnounceActionsMixin):
    def __init__(self, user_id, stream_factory, user_manager_stream_key, event_generator):
        super(Publisher, self).__init__(user_id, 'advertise', 'unadvertise', stream_factory, user_manager_stream_key)
        self.event_generator = event_generator

    def publish_next_event(self):
        event_data = self.event_generator.next_event()
        self.stream.write_events(event_data)

    def start(self):
        super(Publisher, self).start()
        try:
            while True:
                self.publish_next_event()
        finally:
            self.stop()
