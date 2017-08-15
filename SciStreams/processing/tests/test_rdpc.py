from SciStreams.processing.rdpc import RDeltaPhiCorrelator
import numpy as np

def test_rdpc():

    img = np.ones((100, 100))
    origin = 50, 50
    mask = np.ones_like(img)

    # TODO : add more tests, right now just checks it runs fine
    rdpc = RDeltaPhiCorrelator(img.shape, mask=mask, origin=origin)
    rdpc.run(img)
