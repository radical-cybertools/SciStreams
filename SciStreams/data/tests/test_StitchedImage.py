from SciStreams.data.StitchedImage import StitchedImage
# test the stitched images
import numpy as np
from numpy.testing import assert_array_almost_equal


def test_stitched_image():
    ''' This tests the often used StitchedImage object.

        Note : Some of the behaviour is arbitrary. If it changes,
            it is likely acceptable but the test ensures that we know what
            we're doing if we see it and accept the change.
            Some possible cases:
            * stitching two images could result in a new image 1 pixel wider
            than previously. Depending on fringe cases that will come up, this
            might be something desirable etc.
            * Currently stitching is digitized to pixels. If we want sub-pixel
            stitching, some of this behaviour may change. This is something
            intended to add to the future.
    '''
    # create first image
    img1 = np.zeros((10, 10))
    img1[1:3, 4:7] = 1

    # create second image, negative of first
    img2 = img1*-1

    # instantiate the stitched images
    simg1 = StitchedImage(img1, [0, 0])
    simg2 = StitchedImage(img2, [.9, .9])

    # use the addition operator to stitch images
    simg3 = simg1 + simg2

    # check the third image is as expected
    img3 = simg3.image
    assert img3.shape == (img1.shape[0]+1, img1.shape[1]+1)

    assert_array_almost_equal(img3[1:4, 4:8],
                              [[-1, -1, -1, 0],
                               [-1, 0, 0, 1],
                               [0, 1, 1, 1],
                               ]
                              )

    assert_array_almost_equal(simg3.refpoint, (1.9, 1.9))

    # change the reference point to 1 pixel now. This should enlarge stithced
    # image by 1 pixel.
    # when it's 1, it goes over by 2 pixels
    simg2 = StitchedImage(img2, [1, 1])

    simg3 = simg1 + simg2

    img3 = simg3.image
    assert img3.shape == (img1.shape[0]+2, img1.shape[1]+2)
    assert_array_almost_equal(img3[2:5, 5:9],
                              [[-1, -1, -1, 0],
                               [-1, 0, 0, 1],
                               [0, 1, 1, 1],
                               ]
                              )
    assert_array_almost_equal(simg3.refpoint, (2, 2))
