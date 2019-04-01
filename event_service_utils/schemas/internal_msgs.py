import json


class BaseInternalMessage():

    def __init__(self, action=None, json_msg=None):
        self.dict = {
            'action': action
        }
        self.json_serialized = json_msg

    def json_msg_load_from_dict(self):
        if self.dict.get('action') is None:
            self.dict['action'] = ''

        self.json_serialized = {
            'event': json.dumps(self.dict)
        }
        return self.json_serialized

    def object_load_from_msg(self):
        event_json = self.json_serialized.get(b'event', '{}')
        self.dict = json.loads(event_json)
        if self.dict.get('action') is None:
            self.dict['action'] = ''
        return self.dict


class UserManagerAnnounceActionMessage(BaseInternalMessage):
    def __init__(self, uid=None, action=None, subscription=None, json_msg=None):
        super(UserManagerAnnounceActionMessage, self).__init__(action=action, json_msg=json_msg)
        self.dict.update({
            'uid': uid,
            'subscription': subscription,
        })

    def json_msg_load_from_dict(self):
        if not self.dict['subscription']:
            self.dict['subscription'] = ''

        return super(UserManagerAnnounceActionMessage, self).json_msg_load_from_dict()

    def object_load_from_msg(self):
        super(UserManagerAnnounceActionMessage, self).object_load_from_msg()
        if not self.dict['subscription']:
            self.dict['subscription'] = ''
        return self.dict


class EventDispatcherUpdatePublisherMessage(BaseInternalMessage):
    def __init__(self, uid=None, action=None, json_msg=None):
        super(EventDispatcherUpdatePublisherMessage, self).__init__(action=action, json_msg=json_msg)
        self.dict.update({
            'uid': uid,
        })


class MatchingEngineUpdateSubscriberMessage(BaseInternalMessage):
    def __init__(self, uid=None, action=None, sub_id=None, subscription=None, json_msg=None):
        super(MatchingEngineUpdateSubscriberMessage, self).__init__(action=action, json_msg=json_msg)
        self.dict.update({
            'uid': uid,
            'sub_id': sub_id,
            'subscription': subscription
        })


class ProcessorManagerAvailableProcessorsMessage(BaseInternalMessage):
    def __init__(self, available_processors=None, action=None, json_msg=None):
        super(ProcessorManagerAvailableProcessorsMessage, self).__init__(action=action, json_msg=json_msg)
        self.dict.update({
            'available_processors': available_processors
        })
