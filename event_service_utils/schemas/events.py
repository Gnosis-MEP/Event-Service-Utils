import json
import base64


class BaseEventMessage():

    def __init__(self, id=None, source=None, destinations=None, json_msg=None):
        self.dict = {
            'id': id,
            'source': source,
            'destinations': destinations,
        }
        self.json_serialized = json_msg

    def json_msg_load_from_dict(self):
        if self.dict['destinations'] is None:
            self.dict['destinations'] = []
        if self.dict['id'] is None:
            self.dict['id'] = ''

        self.json_serialized = {
            'event': json.dumps(self.dict)
        }
        return self.json_serialized

    def object_load_from_msg(self):
        event_json = self.json_serialized.get(b'event', '{}')
        self.dict = json.loads(event_json)
        if self.dict['destinations'] is None:
            self.dict['destinations'] = []
        if self.dict['id'] is None:
            self.dict['id'] = ''

        return self.dict


class EventImageByteArrayMessage(BaseEventMessage):

    def __init__(self, id=None, image=None, source=None, destinations=None, filter_results=None, json_msg=None):
        super(EventImageByteArrayMessage, self).__init__(
            id=id, source=source, destinations=destinations, json_msg=json_msg)

        self.dict.update({
            'image': image,
            'source': source,
            'destinations': destinations,
            'filter_results': filter_results,
        })

    def object_load_from_msg(self):
        super(EventImageByteArrayMessage, self).object_load_from_msg()
        self.dict['image'] = self._image_bytes_from_utf8_str(self.dict['image'])

        if self.dict['filter_results'] is None:
            self.dict['filter_results'] = {}

        return self.dict

    def json_msg_load_from_dict(self):
        self.dict['image'] = self._image_bytes_to_utf8_str(self.dict['image'])

        if self.dict['filter_results'] is None:
            self.dict['filter_results'] = {}

        super(EventImageByteArrayMessage, self).json_msg_load_from_dict()
        return self.json_serialized

    def _image_bytes_to_utf8_str(self, image_bytes):
        b64 = base64.b64encode(image_bytes)
        utf_8 = b64.decode('utf-8')
        return utf_8

    def _image_bytes_from_utf8_str(self, image_str):
        enc_image_bytes = image_str.encode('utf-8')
        image_bytes = base64.b64decode(enc_image_bytes)
        return image_bytes


class EventImageURLMessage(BaseEventMessage):

    def __init__(self, id=None, image_url=None, source=None, destinations=None, filter_results=None, json_msg=None):
        super(EventImageURLMessage, self).__init__(
            id=id, source=source, destinations=destinations, json_msg=json_msg)

        self.dict.update({
            'image_url': image_url,
            'source': source,
            'destinations': destinations,
            'filter_results': filter_results,
        })

    def object_load_from_msg(self):
        super(EventImageURLMessage, self).object_load_from_msg()
        if self.dict['filter_results'] is None:
            self.dict['filter_results'] = {}

        return self.dict

    def json_msg_load_from_dict(self):
        if self.dict['filter_results'] is None:
            self.dict['filter_results'] = {}

        super(EventImageURLMessage, self).json_msg_load_from_dict()
        return self.json_serialized


class EventVEkgMessage(EventImageURLMessage):

    def __init__(self, id=None, image_url=None, source=None, destinations=None, vekg=None, json_msg=None):
        super(EventVEkgMessage, self).__init__(
            id=id, image_url=image_url, source=source, destinations=destinations, json_msg=json_msg)

        self.dict.update({
            'vekg': vekg,
        })

    def object_load_from_msg(self):
        super(EventVEkgMessage, self).object_load_from_msg()
        if self.dict['vekg'] is None:
            self.dict['vekg'] = {}

        return self.dict

    def json_msg_load_from_dict(self):
        if self.dict['vekg'] is None:
            self.dict['vekg'] = {}

        super(EventVEkgMessage, self).json_msg_load_from_dict()
        return self.json_serialized


class EventWindowMessage(BaseEventMessage):

    def __init__(self, id=None, event_img_urls=None, sub_id=None, query=None, vekg_stream=None,
                 source=None, destinations=None, json_msg=None):
        super(EventWindowMessage, self).__init__(
            id=id, source=source, destinations=destinations, json_msg=json_msg)

        self.dict.update({
            'sub_id': sub_id,
            'event_img_urls': event_img_urls,
            'query': query,
            'vekg_stream': vekg_stream,
            'source': source,
            'destinations': destinations,
        })

    def object_load_from_msg(self):
        super(EventWindowMessage, self).object_load_from_msg()
        if self.dict['event_img_urls'] is None:
            self.dict['event_img_urls'] = []
        if self.dict['vekg_stream'] is None:
            self.dict['vekg_stream'] = []
        if self.dict['query'] is None:
            self.dict['query'] = {}

        return self.dict

    def json_msg_load_from_dict(self):
        if self.dict['event_img_urls'] is None:
            self.dict['event_img_urls'] = []
        if self.dict['vekg_stream'] is None:
            self.dict['vekg_stream'] = []
        if self.dict['query'] is None:
            self.dict['query'] = {}

        super(EventWindowMessage, self).json_msg_load_from_dict()
        return self.json_serialized
