from cmsdb import cmsdb_analysis
from uuid import uuid4
from PIL import Image
import numpy as np

def save_image(img, filename):
    im = Image.fromarray(img)
    im.save(filename)
    return str(uuid4())

img = (np.random.random((100,100))*255).astype(np.uint8)
_SPEC = "PNG"
outfile = "dummy.png"
dat_uid = save_image(img, outfile)

# save dummy data in a file
fs1 = cmsdb_analysis.fs
resource = fs1.insert_resource(_SPEC, outfile, {})
# don't give kwargs (not necessary for this one
fs1.insert_datum(resource, dat_uid, {})

class PNGHandler:
    def __init__(self, fpath, **kwargs):
        self.fpath = fpath

    def __call__(self, **kwargs):
        return np.array(Image.open(self.fpath))

# retrieving
fs1.deregister_handler(_SPEC)
fs1.register_handler(_SPEC, PNGHandler)
# use the retrieve function to get the data back in ram
data = fs1.retrieve(str(dat_uid))
