from SciAnalysis.analyses.XSAnalysis.Data import Obstruction
import numpy as np
from pylab import *

def test_obstruction():
    image1 = np.zeros((100,100))
    image1[10:30, : ] = 1
    image1[:,40:70] = 1
    origin1 = 10,20

    image2 = np.zeros((100,100))
    image2[10:30, : ] = 1
    image2[:,40:70] = 1

    origin2 = 50,80

    obs1 = Obstruction(image1, origin1)
    obs2 = Obstruction(image2, origin2)
    obs2 = obs2.rotate(10)

    obs3 = obs1 + obs2

    # test subtraction
    obs4 = obs1 - obs2
    ion()
    figure(0);clf();imshow(obs1.image)
    plot(obs1.origin[1], obs1.origin[0], 'go')
    gca().set_aspect("equal")
    figure(1);clf();imshow(obs2.image)
    plot(obs2.origin[1], obs2.origin[0], 'go')
    gca().set_aspect("equal")
    figure(2);clf();imshow(obs3.image)
    plot(obs3.origin[1], obs3.origin[0], 'go')
    gca().set_aspect("equal")

    figure(3);clf();imshow(obs4.image)
    plot(obs4.origin[1], obs4.origin[0], 'go')
    gca().set_aspect("equal")

    # should give mask
    print("getting mask shape: " )
    print(obs1.mask.shape)


def test_beamstop():
    # make a beamstop
    img = np.zeros((100, 100))
    bstop_orig = 20, 30
    circle_rad = 5
    # add the circle
    x = np.arange(100)
    X, Y = np.meshgrid(x,x)
    R = np.sqrt((X-bstop_orig[1])**2 + (Y-bstop_orig[0])**2)
    w = np.where(R < circle_rad)
    img[w] = 1

    # add some horiz beamstop:
    x0, x1 = bstop_orig[1], bstop_orig[1] + 30
    y0, y1 = bstop_orig[0]-3, bstop_orig[0] + 3
    img[y0:y1, x0:x1] = 1

    # add some vertbeamstop:
    x0, x1 = bstop_orig[1]-2, bstop_orig[1] + 2
    y0, y1 = bstop_orig[0], bstop_orig[0] + 50
    img[y0:y1, x0:x1] = 1

    obs = Obstruction(img, bstop_orig)
    obs2 = obs.rotate(10)

    ion();
    figure(1);clf()
    imshow(obs.image)

    figure(2);clf()
    imshow(obs2.image)
