import io
import urllib.request


def image_to_bytes(img):
    img_byte_io = io.BytesIO()
    img.save(img_byte_io, "PNG")
    img_byte_io.seek(0)
    img_bytes = img_byte_io.read()
    return img_bytes


def image_to_bytes_io_and_size(img):
    img_byte_io = io.BytesIO()
    img.save(img_byte_io, "PNG")
    image_file_size = img_byte_io.tell()
    img_byte_io.seek(0)
    return img_byte_io, image_file_size


def image_bytes_buffer_from_url(img_url):
    req = urllib.request.Request(img_url)
    response = urllib.request.urlopen(req)
    return io.BytesIO(response.read())
