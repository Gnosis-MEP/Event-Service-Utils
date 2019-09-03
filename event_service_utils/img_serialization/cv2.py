import numpy as np


DEFAULT_DTYPE = np.dtype('uint8')


def cv2_from_pil_image(pil_image):
    rgb_image = pil_image.convert('RGB')
    rgb_array = np.array(rgb_image)
    cv2_image_bgr = rgb_array[:, :, ::-1].copy()  # RGB -> BGR
    return cv2_image_bgr


def nd_array_from_ndarray_bytes(ndaray_bytes, nd_shape, dtype=DEFAULT_DTYPE):
    ndarray = np.frombuffer(ndaray_bytes, dtype=dtype).reshape(nd_shape)
    return ndarray
