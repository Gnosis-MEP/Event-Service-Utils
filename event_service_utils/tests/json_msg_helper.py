import uuid
import json


def make_dict_key_bites(d):
    return {k.encode('utf-8'): v for k, v in d.items()}


def prepare_event_msg_tuple(msg_dict):
    return (str(uuid.uuid4()), make_dict_key_bites({'event': json.dumps(msg_dict)}))
