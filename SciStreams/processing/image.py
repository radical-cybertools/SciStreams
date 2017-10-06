from scipy.ndimage.filters import gaussian_filter


def blur(image, sigma=None):
    ''' blur the image

        Uses a Gaussian blurring method.

        Parameters
        ----------
        image : 2d np.ndarray
            the image to blur

        sigma : float, optional
            the std dev of the Gaussian kernel to blur with
    '''
    if sigma is not None:
        image = gaussian_filter(image, sigma)
    return dict(image=image)


def crop(image, crop=None):
    ''' Crop an image according to a selection.

        Parameters
        ----------
        image : 2d np.ndarray
            the image to crop
        crop : 4 element list, optional
            [x0, x1, y0, y1] list where selection is
                image[y0:y1, x0:x1]
            default is to return untouched image

    '''
    if crop is not None:
        x0, x1, y0, y1 = crop
        image = image[int(y0):int(y1), int(x0):int(x1)]
    return dict(image=image)


def resize(image, resize=None):
    ''' Performs simple pixel binning

        Parameters
        ----------

        image : 2d np.ndarray
            the image to resize
        resize : int, optional
            the number to bin by
            resize=2 bins 2x2 pixels
        resize must be an integer > 1 and also smaller than the image shape
    '''
    from skimage.measure import block_reduce
    if resize is not None:
        if image.ndim == 2:
            image = block_reduce(image, (resize, resize))
        else:
            raise ValueError("Error, image is not a 2D np.ndarray")

    return dict(image=image)
