import numpy as np
from scipy.ndimage.filters import gaussian_filter


def blur(img, sigma=None, **kwargs):
    ''' blur the image

        Uses a Gaussian blurring method.

        Parameters
        ----------
        img : 2d np.ndarray
            the image to blur

        sigma : float, optional
            the std dev of the Gaussian kernel to blur with
    '''
    if sigma is not None:
        img = gaussian_filter(img, sigma)
    return img


def crop(img, crop=None, **kwargs):
    ''' Crop an image according to a selection.

        Parameters
        ----------
        img : 2d np.ndarray
            the image to crop
        crop : 4 element list, optional
            [x0, x1, y0, y1] list where selection is
                img[y0:y1, x0:x1]
            default is to return untouched image

    '''
    if crop is not None:
        x0, x1, y0, y1 = crop
        img = img[int(y0):int(y1), int(x0):int(x1)]
    return img


def resize(img, resize=None, **kwargs):
    ''' Performs simple pixel binning

        Parameters
        ----------

        img : 2d np.ndarray
            the image to resize
        resize : int, optional
            the number to bin by
            resize=2 bins 2x2 pixels
        resize must be an integer > 1 and also smaller than the image shape
    '''
    newimg = img
    if resize is not None:
        resize = int(resize)
        if resize > 1:
            # cut off edges
            newimg = np.zeros_like(img[resize-1::resize, resize-1::resize])
            newdims = np.array(newimg.shape)*resize
            img = img[:newdims[0], :newdims[1]]
            for i in range(resize):
                for j in range(resize):
                    newimg += img[i::resize, j::resize]
            newimg = newimg/resize**2
    return newimg
