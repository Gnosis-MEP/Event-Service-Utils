import io

from event_service_utils.img_serialization.base import image_bytes_buffer_from_url

from PIL import Image


def load_img_from_file(img_path):
    return Image.open(img_path)


def image_from_bytes(img_bytes):
    img_bytes_io = io.BytesIO(img_bytes)
    return Image.open(img_bytes_io)


def load_img_from_url(img_url):
    img = Image.open(image_bytes_buffer_from_url(img_url))
    return img


def image_from_nd_array(nd_array):
    pil_img = Image.fromarray(nd_array[:, :, ::-1].copy())
    return pil_img
