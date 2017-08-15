from ...data.Singlet import Singlet
from collections import ChainMap

import numpy as np

from ...interfaces.detectors import detectors2D

from scipy.interpolate import RegularGridInterpolator
from scipy.ndimage.interpolation import rotate as scipy_rotate

from .tools import xystitch_accumulate, roundbydigits

from dask.delayed import tokenize
'''
    def run_default(protocol_name, xml=True, file=True, databroker=True, delay=
    True, xml_options=None, file_options=None, databroker_options=None):
'''
# TODO : need to handle classes better (figure out if it's a class, and parse
# sciresults properly)

# @run_default("XSAnalysis_Mask", False, False, False, True)


