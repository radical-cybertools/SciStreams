import numpy as np


def gen_image(origin=None, A=1, sym=6, phi=0., shape=None):
    ''' Generate image from pipeline.

        Parameters
        ----------
        origin : the origin of the image

    '''
    # 256 x 256 image, with origin and mask
    if shape is None:
        shape = 256, 256
    if origin is None:
        origin = shape[0]/2., shape[1]/2.

    x = np.arange(shape[1])
    y = np.arange(shape[0])
    X, Y = np.meshgrid(x - origin[0], y - origin[1])
    R = np.maximum(1e-6, np.hypot(X, Y))
    PHI = np.arctan2(Y, X)

    if A < 0:
        A = A*-1
    IMG = np.random.poisson(1e6*A*np.cos(sym*PHI+phi)**2/R**2)
    # make a 32 bit integer
    IMG = IMG.astype(np.int32)

    return IMG
