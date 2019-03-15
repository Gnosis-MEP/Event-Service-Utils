import numpy as np


def cv2_from_pil_image(pil_image):
    rgb_image = pil_image.convert('RGB')
    rgb_array = np.array(rgb_image)
    cv2_image_bgr = rgb_array[:, :, ::-1].copy()  # RGB -> BGR
    return cv2_image_bgr
