from event_service_utils.schemas.internal_msgs import UserManagerAnnounceActionMessage


class PubSubAnnounceActionsMixin():
    def __init__(self, user_id, join_action, leave_action, stream_factory, user_manager_stream_key='um_cmd'):
        self.user_id = user_id
        self.join_action = join_action
        self.leave_action = leave_action
        self.stream_factory = stream_factory
        self.stream = self.stream_factory.create(self.user_id)
        self.user_manage_stream = self.stream_factory.create(user_manager_stream_key)
        self.subscription = None

    def start(self):
        self.announce_user_action(self.join_action)

    def announce_user_action(self, action):
        event = UserManagerAnnounceActionMessage(uid=self.user_id, action=action, subscription=self.subscription)
        json_event = event.json_msg_load_from_dict()
        return self.user_manage_stream.write_events(json_event)

    def stop(self):
        self.announce_user_action(self.leave_action)
