import datetime
import os
import time
import uuid

from PIL import Image

import imageio
from minio import Minio
from minio.error import (ResponseError, BucketAlreadyOwnedByYou,
                         BucketAlreadyExists)

from event_service_utils.schemas.events import EventVEkgMessage, EventWindowMessage
from event_service_utils.event_generators_processors.base import BaseEventProcessor, BaseEventGenerator
from event_service_utils.img_serialization.base import image_to_bytes_io_and_size
from event_service_utils.img_serialization.pil import load_img_from_url, load_img_from_file
from event_service_utils.img_serialization.cv2 import cv2_from_pil_image
from event_service_utils.img_serialization.redis import RedisImageCache


class MinioMixing():

    def initialize_file_storage_client(self):
        self.fs_client = Minio(
            **self.file_storage_cli_config
        )

    def _create_bucket_for_publisher(self):
        try:
            self.fs_client.make_bucket(self.source)
        except BucketAlreadyOwnedByYou as err:
            pass
        except BucketAlreadyExists as err:
            pass
        except ResponseError as err:
            raise err

    def upload_file_to_storage(self, img_path):
        img_name = os.path.basename(img_path)

        expiration_time = datetime.timedelta(minutes=10)
        try:
            ret = self.fs_client.fput_object(self.source, img_name, img_path, 'image/jpeg')
            ret = self.fs_client.presigned_get_object(self.source, img_name, expires=expiration_time)
        except ResponseError as err:
            raise err
        return ret

    def upload_inmemory_to_storage(self, pil_img):
        img_name = str(uuid.uuid4())
        bytes_io, length = image_to_bytes_io_and_size(pil_img)

        expiration_time = datetime.timedelta(minutes=10)
        try:
            ret = self.fs_client.put_object(
                bucket_name=self.source,
                object_name=img_name,
                length=length,
                data=bytes_io,
                content_type='image/jpeg'
            )
            ret = self.fs_client.presigned_get_object(self.source, img_name, expires=expiration_time)
        except ResponseError as err:
            raise err
        return ret


class ImageRedisCacheEventGenerator(BaseEventGenerator, RedisImageCache):

    def __init__(self, loop, file_storage_cli_config, imgs_dir, source):
        self.loop = loop
        self.file_storage_cli_config = file_storage_cli_config
        self.initialize_file_storage_client()
        self.imgs_dir = imgs_dir
        BaseEventGenerator.__init__(
            self, source=source, event_schema=EventVEkgMessage)
        if self.loop:
            self.imgs_loop = list(os.walk(self.imgs_dir))[0][2]
            self.last_id = -1

    def get_next_image_id(self):
        if self.loop:
            self.last_id += 1
            if self.last_id >= len(self.imgs_loop):
                self.last_id = 0
            time.sleep(0.4)
            return self.imgs_loop[self.last_id]
        else:
            return input(f'Next image name ({self.imgs_dir}/): ')

    def next_event(self):
        img_id = self.get_next_image_id()
        img_path = self.get_image_path(img_id)
        img = load_img_from_file(img_path)
        obj_data = self.upload_inmemory_to_storage(img)
        img_url = obj_data
        event_id = f'{self.source}-{str(uuid.uuid4())}'
        schema = self.event_schema(id=event_id, vekg={}, image_url=img_url, source=self.source)
        return schema.json_msg_load_from_dict()

    def get_image_path(self, img_name):
        return os.path.join(self.imgs_dir, img_name)


class ImageRedisCacheFromMpeg4EventGenerator(BaseEventGenerator, RedisImageCache):
    def __init__(self, file_storage_cli_config, media_source, source):
        self.file_storage_cli_config = file_storage_cli_config
        self.initialize_file_storage_client()
        self.media_source = media_source
        self.reader = imageio.get_reader(media_source)  # <video0> for webcam

        BaseEventGenerator.__init__(
            self, source=source, event_schema=EventVEkgMessage)

    def next_event(self):
        time.sleep(0.38)
        print('new frame')
        try:
            frame = self.reader.get_next_data()
        except imageio.core.format.CannotReadFrameError as e:
            del self.reader
            self.reader = imageio.get_reader(self.media_source)
            print('reseting video...')
            frame = self.reader.get_next_data()
        pil_img = Image.fromarray(frame)

        event_id = f'{self.source}-{str(uuid.uuid4())}'
        obj_data = self.upload_inmemory_to_storage(pil_img)

        img_url = obj_data
        schema = self.event_schema(id=event_id, vekg={}, image_url=img_url, source=self.source)
        return schema.json_msg_load_from_dict()


class ImageFileUploadedCloudStorageEventGenerator(BaseEventGenerator, MinioMixing):

    def __init__(self, loop, file_storage_cli_config, imgs_dir, source):
        self.loop = loop
        self.file_storage_cli_config = file_storage_cli_config
        self.initialize_file_storage_client()
        self.imgs_dir = imgs_dir
        BaseEventGenerator.__init__(
            self, source=source, event_schema=EventVEkgMessage)
        self._create_bucket_for_publisher()
        if self.loop:
            self.imgs_loop = list(os.walk(self.imgs_dir))[0][2]
            self.last_id = -1

    def get_next_image_id(self):
        if self.loop:
            self.last_id += 1
            if self.last_id >= len(self.imgs_loop):
                self.last_id = 0
            time.sleep(0.26)
            return self.imgs_loop[self.last_id]
        else:
            return input(f'Next image name ({self.imgs_dir}/): ')

    def next_event(self):
        img_id = self.get_next_image_id()
        print(f"next frame: {img_id}")
        img_path = self.get_image_path(img_id)
        obj_data = self.upload_file_to_storage(img_path)
        img_url = obj_data
        event_id = f'{self.source}-{str(uuid.uuid4())}'
        schema = self.event_schema(id=event_id, vekg={}, image_url=img_url, source=self.source)
        return schema.json_msg_load_from_dict()

    def get_image_path(self, img_name):
        return os.path.join(self.imgs_dir, img_name)


class ImageUploadFromMpeg4EventGenerator(BaseEventGenerator, MinioMixing):
    def __init__(self, file_storage_cli_config, media_source, source, sleep=0.12):
        self.sleep = sleep
        self.file_storage_cli_config = file_storage_cli_config
        self.initialize_file_storage_client()
        self.media_source = media_source
        self.reader = imageio.get_reader(media_source)  # <video0> for webcam
        self.last_frame_time = time.time()

        BaseEventGenerator.__init__(
            self, source=source, event_schema=EventVEkgMessage)
        self._create_bucket_for_publisher()

    def initialize_file_storage_client(self):
        self.fs_client = Minio(
            **self.file_storage_cli_config
        )

    def next_event(self):
        if self.sleep:
            time.sleep(self.sleep)

        try:
            frame = self.reader.get_next_data()
        except imageio.core.format.CannotReadFrameError as e:
            del self.reader
            self.reader = imageio.get_reader(self.media_source)
            print('reseting video...')
            frame = self.reader.get_next_data()
        pil_img = Image.fromarray(frame)

        event_id = f'{self.source}-{str(uuid.uuid4())}'
        obj_data = self.upload_inmemory_to_storage(pil_img)

        img_url = obj_data
        schema = self.event_schema(id=event_id, vekg={}, image_url=img_url, source=self.source)

        end_time = time.time()
        total_time = end_time - self.last_frame_time
        self.last_frame_time = end_time
        print(f'published event id:{event_id}')
        print(f'Total time from last publishing: {total_time:.4f} secs')
        return schema.json_msg_load_from_dict()


class Mpeg4FromRedisCacheWindowEventProcessor(BaseEventProcessor, RedisImageCache):

    def __init__(self, video_player, file_storage_cli_config):
        self.file_storage_cli_config = file_storage_cli_config
        self.initialize_file_storage_client()
        self.video_player = video_player
        super(Mpeg4FromRedisCacheWindowEventProcessor, self).__init__(event_schema=EventVEkgMessage)

    def process(self, event_tuple):
        event_id, json_msg = event_tuple
        event_schema = self.event_schema(json_msg=json_msg)
        event_data = event_schema.object_load_from_msg()
        # for img_key in event_data.get('event_img_urls', []):

        img_key = event_data['image_url']
        width = event_data['width']
        height = event_data['height']
        color_channels = event_data['color_channels']
        n_channels = len(color_channels)
        nd_shape = (int(height), int(width), n_channels)
        frame = self.get_image_ndarray_by_key_and_shape(img_key, nd_shape)
        if frame is not None:
            fps = 0.0
            self.video_player.play_next(frame, fps)


class Mpeg4FromImageURLEventProcessor(BaseEventProcessor):

    def __init__(self, video_player):
        self.video_player = video_player
        super(Mpeg4FromImageURLEventProcessor, self).__init__(event_schema=EventVEkgMessage)

    def process(self, event_tuple):
        event_id, json_msg = event_tuple
        event_schema = self.event_schema(json_msg=json_msg)
        event_data = event_schema.object_load_from_msg()
        img_url = event_data.get('image_url')
        frame = load_img_from_url(img_url)
        cv2_img = cv2_from_pil_image(frame)
        fps = 30
        self.video_player.play_next(cv2_img, fps)
