import uuid

import redis

from event_service_utils.img_serialization.pil import image_from_nd_array
from event_service_utils.img_serialization.cv2 import nd_array_from_ndarray_bytes, DEFAULT_DTYPE


class RedisImageCache():
    def initialize_file_storage_client(self):
        self.client = redis.StrictRedis(**self.file_storage_cli_config)

    def upload_inmemory_to_storage(self, img_numpy_array):
        img_key = str(uuid.uuid4())
        nd_array_bytes = img_numpy_array.tobytes(order='C')

        ret = self.client.set(img_key, nd_array_bytes)
        if ret:
            self.client.expire(img_key, self.expiration_time)
        else:
            raise Exception('Couldnt set image in redis')
        return img_key

    def get_image_ndarray_bytes_by_key(self, img_key):
        ndarray_bytes = self.client.get(img_key)
        return ndarray_bytes

    def get_image_ndarray_by_key_and_shape(self, img_key, shape):
        ndarray_bytes = self.get_image_ndarray_bytes_by_key(img_key)
        if not ndarray_bytes:
            return None
        dtype = DEFAULT_DTYPE
        ndarray = nd_array_from_ndarray_bytes(ndarray_bytes, shape, dtype=dtype)
        return ndarray

    def get_image_ndarray_by_key_widht_height(self, img_key, width, height):
        n_channels = 3
        shape = (height, width, n_channels)
        return self.get_image_ndarray_by_key_and_shape(img_key, shape)

    def get_image_by_key_widht_height(self, img_key, width, height):
        nd_array = self.get_image_ndarray_by_key_widht_height(img_key, width, height)

        img = image_from_nd_array(nd_array)
        return img

    def delete_image_ndarray_by_key(self, img_key):
        return_value = self.client.delete(img_key)
        if not return_value:
            raise Exception("Could'nt remove image: "+img_key+" from redis.")
        return return_value
