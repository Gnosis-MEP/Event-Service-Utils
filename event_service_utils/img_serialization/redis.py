import datetime
import uuid

import redis

from event_service_utils.img_serialization.base import image_to_bytes
from event_service_utils.img_serialization.pil import image_from_bytes


class RedisImageCache():
    def initialize_file_storage_client(self):
        self.client = redis.StrictRedis(**self.file_storage_cli_config)

    def upload_inmemory_to_storage(self, pil_img):
        img_key = str(uuid.uuid4())
        bytes_io = image_to_bytes(pil_img)

        expiration_time = int(datetime.timedelta(minutes=2).total_seconds())

        ret = self.client.set(img_key, bytes_io)
        if ret:
            self.client.expire(img_key, expiration_time)
        else:
            raise Exception('Couldnt set image in redis')
        # try:
        #     ret = self.fs_client.put_object(
        #         bucket_name=self.source,
        #         object_name=img_name,
        #         length=length,
        #         data=bytes_io,
        #         content_type='image/jpeg'
        #     )
        #     ret = self.fs_client.presigned_get_object(self.source, img_name, expires=expiration_time)
        # except ResponseError as err:
        #     raise err

        # img = load_img_from_file('panda.jpg') #PIL img
        # bytes_io = image_to_bytes(img)

        # img_back.show()
        return img_key

    def get_image_by_key(self, img_key):
        bytes_io = self.client.get(img_key)
        if not bytes_io:
            return None
        img = image_from_bytes(bytes_io)
        return img