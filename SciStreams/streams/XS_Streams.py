# TODO : add pixel procesing/thresholding threshold_pixels((2**32-1)-1) # Eiger
# inter-module gaps
# TODO : add thumb

from .. import globals as streams_globals


from dask import set_options
set_options(delayed_pure=True)  # noqa

import numpy as np

from collections import ChainMap
from ..data.Singlet import Singlet

# Sources
from ..detectors import detectors2D

# wrappers for parsing streamdocs
#from ..core.StreamDoc import select, pack, unpack, todict,\
        #add_attributes, psdm, psda

from ..processing.stitching import xystitch_accumulate, xystitch_result
from ..processing.circavg import circavg
from ..processing.qphiavg import qphiavg
from ..processing.image import blur as _blur, crop as _crop, resize as _resize
from ..processing import rdpc

from ..config import config
keymaps = config['keymaps']


'''
    Notes : load should be a separate function
'''

# from SciAnalysis.analyses.XSAnalysis.Data import Calibration
# use RQConv now
from ..data.Calibration import Calibration

#from ..core.streams import Stream

# cache for qmaps
# TODO : clean this up

# NOTE : When defining streams, make sure to place the expected inputs
# and outputs in the docstrings! See stream below for a good example.


# CALIBRATION STREAM CREATION DELETED: put in startup/run_stream_live_shed.py
# for now

def normalize_calib_dict(**md):
    ''' Normalize the calibration parameters to a set of parameters that the
    analysis expects.
        It gives entries like:
            beamx0 : dict(value=a, unit=b)
        etc...
    '''
    keymap_name = md.get("keymap_name", "cms")
    keymap = keymaps[keymap_name]
    for key, val in keymap.items():
        name = val['name']
        if name is not None:
            print("setting {} to {}".format(name, key))
            # swap out temp vals
            tmpval = md.pop(name)
            default_unit = val['default_unit']
            md[key] = dict(value=tmpval, unit=default_unit)

    return md


def _make_detector_name_from_key(name):
    # remove last "_" character
    return name[::-1].split("_", maxsplit=1)[-1][::-1]


def add_detector_info(**md):
    '''
        Add detector information to the metadata, like shape etc.
        This is a useful step for 2D SAXS analysis, before making the
        calibration parameters.

    Expects:

        detector_name : the detector name
        img_shape : tuple, optional
            force the image shape. This is useful when the detector image
                has been transformed (i.e. image stitching)
    '''
    detector_key = md.get('detector_key', None)

    # only do something is there is a detector key
    if detector_key is not None:
        detector_name = _make_detector_name_from_key(detector_key)

        md['detector_name'] = detector_name

        # use the detector info supplied
        # look up in local library
        md['pixel_size_x'] = detectors2D[detector_name]['pixel_size_x']
        md['pixel_size_y'] = detectors2D[detector_name]['pixel_size_y']

        # shape is just a tuple, not a dict(value=...,unit=...)
        if 'shape' not in md:
            md['shape'] = detectors2D[detector_name]['shape']['value']
    else:
        msg = "Warning : no detector key found,"
        msg += " not adding detector information"
        print(msg)

    return md


def make_calibration(**md):
    '''
        Update calibration with all keyword arguments fill in the defaults

        This expects a dictionary of a certain form with certain elements:
            'wavelength'
            'pixel_size_x'
            'sample_det_distance'
            'beamx0'
            'beamy0'

        img_shape : specify arbitrary shape (useful for stitched images)
    '''
    # TODO : move detector stuff into previous load routine
    # k = 2pi/wv
    wavelength = md['wavelength']['value']
    md['k'] = dict(value=2.0*np.pi/wavelength, unit='1/Angstrom')
    # energy
    # h = 6.626068e-34  # m^2 kg / s
    c = 299792458  # m/s
    wavelength = md['wavelength']['value']  # in Angs *1e-10  # m
    # E = h*c/wavelength  # Joules
    # E *= 6.24150974e18  # electron volts
    # E /= 1000.0  # keV
    # calib_tmp.update(Singlet('energy', E, 'keV'))
    # q per pixel (Small angle limit)
    '''Gets the delta-q associated with a single pixel. This is computed in
    the small-angle limit, so it should only be considered a approximate.
    For instance, wide-angle detectors will have different delta-q across
    the detector face.'''
    c = (md['pixel_size_x']['value']/1e6) / \
        md['sample_det_distance']['value']
    twotheta = np.arctan(c)  # radians
    md['q_per_pixel'] = dict(value=2.0*md['k']['value']*np.sin(twotheta/2.0),
                             unit="1/Angstrom")

    # some post calculations

    pixel_size_um = md['pixel_size_x']['value']
    distance_m = md['sample_det_distance']['value']
    wavelength_A = wavelength

    # prepare the calibration object
    calib_object = Calibration(wavelength_A=wavelength_A,
                               distance_m=distance_m,
                               pixel_size_um=pixel_size_um)
    # NOTE : width, height reversed in calibration
    height, width = md['shape']
    calib_object.set_image_size(width, height)
    calib_object.set_beam_position(md['beamx0']['value'],
                                   md['beamy0']['value'])
    #print("calibration object: {}".format(calib_object))
    #print("calibration object members: {}".format(calib_object.__dict__))

    return calib_object


def _generate_qxyz_maps(calib_obj):
    calib_obj.generate_maps()
    return calib_obj


def CircularAverageStream():
    ''' Circular average stream.

        Stream Inputs
        -------------
            (image, calibration, mask=, bins=)

            image : 2d np.ndarray
                the image to run circular average on

            calibration : 2D np.ndarray
                the calibration object, with members:

                q_map : 2d np.ndarray
                    the magnite of the wave vectors

                r_map : 2d np.ndarray
                    the pixel positions from center

            mask= : 2d np.ndarray, optional
                the mask

            bins= : int or tuple, optional
                if an int, the number of bins to divide into
                if a list, the bins to use

        Stream Outputs
        --------------
            sqx= : 1D np.ndarray
                the q values
            sqxerr= : 1D np.ndarray
                the error q values
            sqy= : 1D np.ndarray
                the intensities
            sqyerr= : 1D np.ndarray
                the error in intensities (approximate)

        Returns
        -------
            sin : Stream, the source stream (see Stream Inputs)
            sout : the output stream (see Stream Outputs)

        Notes
        -----
        - Assumes square pixels
        - Assumes variance comes from shot noise only (by taking average along
            ring/Npixels)
        - If bins is None, it does its best to estimate pixel sizes and make
            the bins a pixel in size. Note, for Ewald curvature this is not
            straightforward. You need both a r_map in pixels from the center
            and the q_map for the actual q values.

    TODO : Add options

    '''
    # TODO : extend file to mltiple writers?
    def validate(x):
        if 'args' not in x:
            message = "args not in doc"
            raise ValueError(message)
        if 'kwargs' not in x:
            message = "kwargs not in doc"
            raise ValueError(message)
        if len(x['args']) != 2:
            message = "expected two arguments: "
            message += "(image, calibration), "
            message += "got {} instead".format(len(x['args']))
            raise ValueError(message)
        # kwargs are optional so don't validate them
        return x

    sin = Stream(name="Circular Average Stream")
    s2 = sin.map(validate)
    # s2 = sin.map(add_attributes)  # , stream_name="CircularAverage")
    # s2.map(psdm(lambda x : x)).map(print)

    sout = s2.map(psdm(circavg_from_calibration))
    return sin, sout


def circavg_from_calibration(image, calibration, mask=None, bins=None):
    # print("circavg : qmap : {} ".format(calibration.q_map))
    # print("circavg : rmap: {} ".format(calibration.r_map))
    return circavg(image, q_map=calibration.q_map, r_map=calibration.r_map,
                   mask=mask, bins=bins)


def QPHIMapStream(bins=(400, 400)):
    '''
        Parameters
        ----------
        bins : 2 tuple, optional
            the number of bins to divide into

        Stream Inputs
        -------------
            img : 2d np.ndarray
                the image
            mask : 2d np.ndarray, optional
                the mask
            bins : 2 tuple, optional
                the number of bins to divide into
            origin : 2 tuple
                the beam center in the image

        Stream Outputs
        --------------
            sqphi= : 2d np.ndarray
                the sqphi map
            qs= : 1d np.ndarray
                the q values
            phis= : 1d np.ndarray
                the phi values

        Returns
        -------
        sin : the input stream (see Stream Inputs)
        sout : the output stream (see Stream Outputs)
    '''
    sin = Stream(name="QPHI map Stream")
    sout = sin.map(select, 0, 'mask', 'origin')\
        .map((add_attributes), stream_name="QPHIMapStream")
    sout = sout.map(psdm(qphiavg), bins=bins)
    # from dask import compute
    # sout.apply(compute).apply(print)
    return sin, sout


def AngularCorrelatorStream():
    ''' Stream to run angular correlations.

        Stream Inputs
        -------------
        shape : 2 tuple
            the image shape
        origin : 2 tuple
            the beam center of the image
        mask : 2d np.ndarray
            the mask
        rbins : number, optional
            the number of bins in q
        phibins : number, optional
            the number of bins in phi
        method : string, optional
            the method to use for the angular correlations
            defaults to 'bgest'

        Stream Outputs
        --------------
        sin : the stream input
        sout : the stream output

        Incomplete
    '''
    # from SciAnalysis.analyses.XSAnalysis import rdpc
    # s = Stream()
    # sout.

    # return s, sout
    return None


def prepare_correlation(shape, origin, mask, rbins=800, phibins=360,
                        method='bgest'):
    rdphicorr = rdpc.RDeltaPhiCorrelator(shape,  origin=origin,
                                         mask=mask, rbins=rbins,
                                         phibins=phibins)
    # print("kwargs : {}".format(kwargs))
    return rdphicorr


def angularcorrelation(rdphicorr, image):
    ''' Run the angular correlation on the angular correlation object.'''
    rdphicorr.run(image)
    return rdphicorr.rdeltaphiavg_n


def _xystitch_result(img_acc, mask_acc, origin_acc, stitchback_acc):
    # print("_xystitch_result, img_acc : {}".format(img_acc))
    return xystitch_result(img_acc, mask_acc, origin_acc, stitchback_acc)


def _xystitch_accumulate(prevstate, newstate):
    # print("_xystitch_accumulate, prevstate: {}".format(prevstate))
    return xystitch_accumulate(prevstate, newstate)


# Image stitching Stream
def ImageStitchingStream(return_intermediate=False):
    '''
        Image stitching

        Stream Inputs
        -------------
            image= : 2d np.ndarray
                the image for the stitching
            mask= : 2d np.ndarray
                the mask
            origin= : 2 tuple
                the beam center
            stitchback= : bool
                whether or not to stitchback to previous image

        Stream Outputs
        --------------
            image= : 2d np.ndarray
                the stitched image
            mask= : 2d np.ndarray
                the mask from the stitch
            origin= : 2 tuple
                the beam center
            stitchback= : bool
                whether or not to stitchback to previous image

        Returns
        -------
            sin : the input stream
            sout : the output stream

        Parameters
        ----------
            return_intermediate : bool, optional
                decide whether to return intermediate results or not
                defaults to False

        Notes
        -----
        Any normalization of images (for ex: by exposure time) should be done
        before inputting to this stream.
    '''
    # TODO : add state. When False returned, need a reason why
    def validate(x):
        if not hasattr(x, 'kwargs'):
            raise ValueError("No kwargs")
        kwargs = x['kwargs']
        expected = ['mask', 'origin', 'stitchback', 'image']
        for key in expected:
            if key not in kwargs:
                message = "{} not in kwargs".format(key)
                raise ValueError(message)
        if not isinstance(kwargs['mask'], np.ndarray):
            message = "mask is not array"
            raise ValueError(message)

        if not isinstance(kwargs['image'], np.ndarray):
            message = "image is not array"
            raise ValueError(message)

        if len(kwargs['origin']) != 2:
            message = "origin not length 2"
            raise ValueError(message)
        return x

    # TODO : remove the add_attributes part and just keep stream_name
    sin = Stream(name="Image Stitching Stream", stream_name="ImageStitch")
    s2 = sin.map(validate)
    # sin.map(lambda x : print("Beginning of stream data\n\n\n"))
    # TODO : remove compute requirement
    s2 = s2.map(add_attributes, stream_name="ImageStitch")

    s3 = s2.map(select, ('image', None), ('mask', None), ('origin', None),
                ('stitchback', None))
    sout = s3.map(psdm(pack))
    sout = sout.accumulate(psda(_xystitch_accumulate))

    sout = sout.map(psdm(unpack))
    sout = sout.map(psdm(_xystitch_result))
    sout = sout.map(psdm(todict))

    # now window the results and only output if stitchback from previous is
    # nonzero
    # save previous value, grab previous stitchback value
    swin = sout.sliding_window(2)

    def stitchbackcomplete(xtuple):
        ''' only plot images whose stitch is complete, and only involved more
        than one image
        NOTE : *Only* the bool "True" will activate a stitch. "1" does not
        count. This is handled by checking 'is True' and 'is not True'
        '''
        # previous must have been true for stitch to have involved more than
        # one image
        prev = xtuple[0]['kwargs']['stitchback']
        # next must be False (or just not True) to be complete
        next = xtuple[1]['kwargs']['stitchback']

        return next is not True and prev is True

    # swin.map(lambda x : print("result : {}".format(x)), raw=True)

    # only get results where stitch is stopped
    # NOTE : need to compute before filtering here

    # now emit some dummy value to swin, before connecting more to stream
    # swin.emit(dict(attributes=dict(stitchback=0)))

    if not return_intermediate:
        swinout = swin.filter(stitchbackcomplete)
    else:
        swinout = swin

    def getprevstitch(x):
        x0 = x[0]
        return x0

    swinout = swinout.map(getprevstitch)
    # swinout.map(lambda x : print("End of stream data\n\n\n"))

    return sin, swinout


def ThumbStream(blur=None, crop=None, resize=None):
    ''' Thumbnail stream

        Parameters
        ----------
            blur : float, optional
                the sigma of the Gaussian kernel to convolve image with
                    for smoothing
                default is None, no smoothing

            crop : 4 tuple of int, optional
                the boundaries to crop by
                default is None, no cropping

            resize : int, optional
                the factor to resize by
                for example resize=2 performs 2x2 binning of the image

        Stream Inputs
        -------------
            image : 2d np.ndarray
                the image

        Returns
        -------
            sin : the stream input
            sout : the stream output

    '''
    # TODO add flags to actually process into thumbs
    sin = Stream(name="Thumbnail Stream")
    s0 = sin.map((add_attributes), stream_name="Thumb")
    # s1 = sin.add_attributes(stream_name="ThumbStream")
    s1 = s0.map(psdm(_blur))
    s1 = s1.map(psdm(_crop))
    sout = s1.map(psdm(_resize)).map(select, (0, 'thumb'))

    return sin, sout
