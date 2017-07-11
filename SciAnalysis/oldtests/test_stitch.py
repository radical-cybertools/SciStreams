import numpy as np
from pylab import *

from SciAnalysis.analyses.XSAnalysis.tools import xystitch_accumulate

# test image stitching
imga = np.ones((100,100))
imgb = np.ones((100,100))
maska = np.ones((100,100))
maskb = np.ones((100,100))

imga[10:20, :] = 0

imgb[:, 40:50] = 0

origina = (10,10)
originb = (80,80)


prevstate = imga, maska, origina, 1
newstate = imgb, maskb, originb, 1

newstate = xystitch_accumulate(prevstate, newstate)
imgc, maskc, originc, stitchc = newstate


ion()
figure(0);clf();
imshow(imga)
plot(origina[1], origina[0], 'go')
figure(1);clf();
imshow(imgb)
plot(originb[1], originb[0], 'go')
figure(2);clf();
imshow(imgc)
plot(originc[1], originc[0], 'go')
