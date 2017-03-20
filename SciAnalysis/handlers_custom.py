import tifffile
from filestore.handlers_base import HandlerBase
import numpy as np
from PIL import Image

class AreaDetectorTiffHandler(HandlerBase):
    specs = {'AD_TIFF'} | HandlerBase.specs
    def __init__(self, fpath, template, filename, frame_per_point=1,
                ROOTMAP={}):
        # This is hard coded change for CMS Area detector
        for key, value in ROOTMAP.items():
            fpath = fpath.replace(key, value)
        self._path = fpath
        self._fpp = frame_per_point
        self._template = template
        self._filename = filename

    def _fnames_for_point(self, point_number):
        start, stop = point_number * self._fpp, (point_number + 1) * self._fpp
        for j in range(start, stop):
            yield self._template % (self._path, self._filename, j)

    def __call__(self, point_number):
        ret = []
        for fn in self._fnames_for_point(point_number):
            with tifffile.TiffFile(fn) as tif:
                ret.append(tif.asarray())
        return np.array(ret).squeeze()

    def get_file_list(self, datum_kwargs):
        ret = []
        for d_kw in datum_kwargs:
            ret.extend(self._fnames_for_point(**d_kw))
        return ret

# create quick handler
class PNGHandler:
    def __init__(self, fpath, **kwargs):
        self.fpath = fpath

    def __call__(self, **kwargs):
        return np.array(Image.open(self.fpath))
