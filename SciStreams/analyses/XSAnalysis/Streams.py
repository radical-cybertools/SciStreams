#!/usr/bin/python
# -*- coding: utf-8 -*-
# vi: ts=4 sw=4

from ...globals import cache, client
cache.register()  # noqa


from dask import compute

from dask import set_options
set_options(delayed_pure=True)  # noqa

import numpy as np

from dask.delayed import delayed

from collections import ChainMap
from ...data.Singlet import Singlet

# Sources
from ...interfaces.detectors import detectors2D

from ...interfaces.StreamDoc import Arguments


'''
    Notes : load should be a separate function
'''

# from SciAnalysis.analyses.XSAnalysis.Data import Calibration
# use RQConv now
from .DataRQconv \
        import CalibrationRQconv as Calibration

from ...interfaces.StreamDoc import StreamDoc
from ...interfaces.streams import Stream

from collections import deque


def add_attributes(sdoc, **attr):
    newsdoc = StreamDoc(sdoc)
    newsdoc.add(attributes=attr)
    return newsdoc

# cache for qmaps
# TODO : clean this up
QMAP_CACHE = deque(maxlen=1000)


# Calibration for SAXS data
# NOTE : Makes the assumption that the wrapper provides 'select' functionality
def CalibrationStream(keymap_name=None, detector=None):  # , wrapper=None):
    '''
        This returns a stream of calibration methods.

        Input:
            A calibration dictionary

        Optional parameters:
            wrapper : specify a wrapper function
                usually something like delayed
                This is stream-specific

            keymap_name : the name of the keymap to use
                for now, it's just 'cms' for cms data

            detector : the detector name
                for ex : 'pilatus300' for SAXS detector


        Output:
            A tuple of the source and dictionary of streams:
                calibration : the calibration stream
                q_maps : the zipped q_maps stream, containing:
                    qx_map : a qx_map
                    qy_map : a qy_map
                    qz_map : a qz_map
                    qr_map : a qr_map
                    q_map  : the magnitude sqrt(qx**2 + qy**2 + qz**2)
                origin : the origin
    '''
    if keymap_name is None:
        keymap_name = "cms"

    if detector is None:
        detector = 'pilatus300'

    # getting some hard-coded defaults
    keymap, defaults = _get_keymap_defaults(keymap_name)

    def validate(input_data):
        if 'args' not in input_data:
            return dict(state=False, message="args not in StreamDoc")
        args = input_data['args']
        if len(args) != 1:
            return dict(state=False, message="args not length 1")
        data = args[0]
        # if 'sample_savename' not in kwargs:
        # return False
        for key, value in keymap.items():
            if value not in data:
                message = "{} not in dict with keys {}"\
                    .format(value, list(data.keys()))
                return dict(state=False, message=message)
        return True

    # the pipeline flow defined here
    sin = Stream(validator=validate)
    # s2 = dask_streams.scatter(sin)
    s2 = sin.map(add_attributes, stream_name="Calibration", raw=True)
    # s2 = dask_streams.gather(s2)
    # s2.map(compute, raw=True).map(print, raw=True)
    calib = s2.map(load_calib_dict, keymap=keymap, defaults=defaults)
    # calib.map(compute, raw=True).map(print, raw=True)
    # calib = calib.map(load_from_calib_dict, detector=detector,
    # calib_defaults=defaults)
    calib_obj = calib.map(load_from_calib_dict, detector=detector,
                          calib_defaults=defaults)
    # calib_obj.apply(compute).apply(print)

    # q_maps = calib.map(_generate_qxyz_maps)
    calib_obj = calib_obj.map(delayed, raw=True, pure=True)\
        .map(_generate_qxyz_maps)
    # calib_obj = calib_obj.map(_generate_qxyz_maps)
    # calib_obj.map(print, raw=True)

    def printcache(obj):
        from ...globals import cache
        print("cache is {}".format(cache.cache.data))
        return obj
    # calib_obj.map(printcache, raw=True)
    # calib_obj.apply(compute).apply(lambda x : print("in cliabraion:
        # {}".format(x)))
    # for distributed regime, store intermediate values
    # sink the cache for the qmaps
    # q_maps.apply(client.compute).sink(QMAP_CACHE.append)
    calib_obj.map(client.compute, raw=True).sink(QMAP_CACHE.append)
    calib_obj = calib_obj.map(lambda x: compute(x)[0], raw=True)
    # calib_obj.map(print, raw=True)
    # compute it so that it's cached on cluster
    # TODO : figure out best way to make this dask and non dask compatible
    # q_maps.apply(print)
    # q_maps.apply(compute, pure=True)
    # qx_map, qy_map, qz_map, qr_map = q_maps.multiplex(4)
    # just select first 3 args
    # TODO : add to  FAQ "Integer tuple pairs not accepted" when giving (0,1,2)
    # for ex instaead of [0,1,2]
    # q_map = q_maps.select(0,1,2).map(_generate_q_map)
    # angle_map = calib.map(_generate_angle_map)
    # r_map = calib.map(_generate_r_map).select((0, 'r_map'))
    # origin = calib.map(get_beam_center)

    # make final qmap stream
    # q_maps = q_maps.select((0, 'qx_map'), (1, 'qy_map'), (2, 'qz_map'), (3,
    # 'qr_map'))
    # q_maps = q_maps.merge(q_map.select((0, 'q_map')), r_map)
    # q_maps = q_maps.merge(angle_map.select((0, 'angle_map')))

    # rename to kwargs (easier to inspect)
    # calib = calib.select((0, 'calibration'))

    # they're relative sinks
    # sout = dict(calibration=calib, q_maps=q_maps, origin=origin)
    sout = calib_obj
    # return sin and the endpoints
    return sin, sout


def get_beam_center(obj):
    ''' Get beam center in row, col (y,x) format.'''
    x0 = obj['beamx0']['value']
    y0 = obj['beamy0']['value']
    return (y0, x0),


def _get_keymap_defaults(name):
    ''' Get the keymap for the calibration, along with default values.'''
    # print("_get_keymap_defaults, name : {}".format(name))
    if name == "cms":
        keymap = {
                    'wavelength': 'calibration_wavelength_A',
                    'beamx0': 'detector_SAXS_x0_pix',
                    'beamy0': 'detector_SAXS_y0_pix',
                    'sample_det_distance': 'detector_SAXS_distance_m',
        }
        # ChainMap is not hashed by dask properly
        defaults = dict(ChainMap(Singlet('wavelength', None, 'Angstrom'),
                                 Singlet('detector', 'pilatus300', 'N/A'),
                                 Singlet('beamx0', None, 'pixel'),
                                 Singlet('beamy0', None, 'pixel'),
                                 Singlet('sample_det_distance', None, 'm'),
                                 Singlet('pixel_size_x', None, 'pixel'),
                                 Singlet('pixel_size_y', None, 'pixel'),))
    elif name == "None":
        keymap = {
                    'wavelength': 'wavelength',
                    'beamx0': 'beamx0',
                    'beamy0': 'beamy0',
                    'sample_det_distance': 'sample_det_distance',
        }
        defaults = dict(ChainMap(Singlet('wavelength', None, 'Angstrom'),
                                 Singlet('detectors', ['pilatus300'],
                                         'N/A'), Singlet('beamx0', None,
                                                         'pixel'),
                                 Singlet('beamy0', None, 'pixel'),
                                 Singlet('sample_det_distance', None, 'm'),
                                 Singlet('pixel_size_x', None, 'pixel'),
                                 Singlet('pixel_size_y', None, 'pixel'),))
    else:
        raise ValueError("Error, cannot find keymap for loading"
                         "calibration")

    return keymap, defaults


def load_calib_dict(attributes, keymap=None, defaults=None):
    ''' Load calibration dictionary from attributes.
        Attributes are a dictionary.

        Parameters
        ----------
        attributes : dict
            the attributes for the calibration data
                (wavelength etc.)

        calib_keymap : dict
            the mapping of the keywords for this calibration object
            to the keywords of the attributes dictionary
            some key words necessary:
                wavelength : the wavelength (Angstroms)
                beamx0 : x center of beam (rows) (pixels)
                beamy0 : y center of beam (cols) (pixels)
                sample_det_distance : the sample detector distance (m)
    '''
    # print("load_calib_dict, attributes: {}".format(attributes))
    # print("load_calib_dict, keymap: {}".format(keymap))
    # print("load_calib_dict, defaults: {}".format(defaults))
    if keymap is None:
        keymap, defaults = _get_keymap_defaults("None")
    calib_keymap = keymap
    calib_defaults = defaults

    # TODO Allow for different units
    olddict = attributes
    newdict = dict()
    # print("load_calib_dict, newdict : {}".format(newdict))
    # print("load_calib_dict, calib_keymap: {}".format(calib_keymap))
    for key, newkey in calib_keymap.items():
        try:
            newdict.update(Singlet(key, olddict[newkey],
                                   calib_defaults[key]['unit']))
        except KeyError:
            raise KeyError("There is an entry missing" +
                           " in header : {}.".format(newkey) +
                           "Cannot proceed")
    # TODO : mention dicts need to be returned as tuples or encapsulated in a
    # dict
    # this is new syntax for putting arguments in dict
    # print("load_calib_dict, newdict : {}".format(newdict))
    return newdict


def load_from_calib_dict(calib_dict, detector=None, calib_defaults=None):
    '''
        Update calibration with all keyword arguments fill in the defaults
    '''
    calib_tmp = dict()
    calib_tmp.update(calib_dict)

    # use the detector info supplied
    # look up in local library
    pixel_size_x_val = detectors2D[detector]['pixel_size_x']['value']
    pixel_size_x_unit = detectors2D[detector]['pixel_size_x']['unit']
    pixel_size_y_val = detectors2D[detector]['pixel_size_y']['value']
    pixel_size_y_unit = detectors2D[detector]['pixel_size_y']['unit']

    img_shape = detectors2D[detector]['shape']

    calib_tmp.update(Singlet('pixel_size_x', pixel_size_x_val,
                             pixel_size_x_unit))
    calib_tmp.update(Singlet('pixel_size_y', pixel_size_y_val,
                             pixel_size_y_unit))
    calib_tmp['shape'] = img_shape.copy()

    # some post calculations
    # k = 2pi/wv
    wavelength = calib_tmp['wavelength']['value']
    calib_tmp.update(Singlet('k', 2.0*np.pi/wavelength, '1/Angstrom'))
    # energy
    # h = 6.626068e-34  # m^2 kg / s
    c = 299792458  # m/s
    wavelength = calib_tmp['wavelength']['value']  # in Angs *1e-10  # m
    # E = h*c/wavelength  # Joules
    # E *= 6.24150974e18  # electron volts
    # E /= 1000.0  # keV
    # calib_tmp.update(Singlet('energy', E, 'keV'))
    # q per pixel (Small angle limit)
    '''Gets the delta-q associated with a single pixel. This is computed in
    the small-angle limit, so it should only be considered a approximate.
    For instance, wide-angle detectors will have different delta-q across
    the detector face.'''
    c = (calib_tmp['pixel_size_x']['value']/1e6) / \
        calib_tmp['sample_det_distance']['value']
    twotheta = np.arctan(c)  # radians
    calib_tmp.update(Singlet('q_per_pixel',
                             2.0*calib_tmp['k']['value']*np.sin(twotheta/2.0),
                             "1/Angstrom"))

    # some post calculations
    # calibration = calib_tmp

    pixel_size_um = pixel_size_x_val
    distance_m = calib_tmp['sample_det_distance']['value']
    wavelength_A = wavelength
    calib_object = Calibration(wavelength_A=wavelength_A,
                               distance_m=distance_m,
                               pixel_size_um=pixel_size_um)
    # NOTE : width, height reversed in calibration
    height, width = calib_tmp['shape']['value']
    calib_object.set_image_size(width, height)
    calib_object.set_beam_position(calib_dict['beamx0']['value'],
                                   calib_dict['beamy0']['value'])
    # print("calibration object: {}".format(calib_object))
    # print("calibration object members: {}".format(calib_object.__dict__))

    # return calibration,
    return calib_object


def _generate_qxyz_maps(calib_obj):
    # print("_generate_qxyz_maps calib_obj : {}".format(calib_obj))
    calib_obj.generate_maps()
    # print("_generate_qxyz_maps calib object qxmap shape :
    # {}".format(calib_obj.qx_map.shape))
    # print(calib_obj.origin)
    # MUST return the object if caching
    return calib_obj


def CircularAverageStream():
    ''' Circular average stream.

        Inputs :
            image : 2d np.ndarray
                the image to run circular average on

            q_map : 2d np.ndarray
                the magnite of the wave vectors

            r_map : 2d np.ndarray
                the pixel positions from center

            mask : 2d np.ndarray
                the mask

        Outputs :
            source : Stream, the source stream
            sink : dict, the output returns a dictionary of four elements:
                sqx : the q values
                sqxerr : the error in q values
                sqy : the intensities
                sqyerr : the error in intensities (approximate)

        Notes
        -----
        - Assumes square pixels
        - Assumes variance comes from shot noise only (by taking average along
            ring/Npixels)
        - If bins is None, it does it's best to estimate pixel sizes and make
            the bins a pixel in size. Note, for Ewald curvature this is not
            straightforward. You need both a r_map in pixels from the center
            and the q_map for the actual q values.

    TODO : Add options

    '''
    # TODO : extend file to mltiple writers?
    def validate(x):
        if 'args' not in x:
            return dict(state=False, message="args not in doc")
        if 'kwargs' not in x:
            return dict(state=False, message="kwargs not in doc")
        if len(x['args']) != 2:
            message = "expected two arguments: "
            message += "(image, calibration), "
            message += "got {} instead".format(len(x['args']))
            return dict(state=False, message=message)
        # kwargs are optional so don't validate them
        return True

    sin = Stream(validator=validate)
    s2 = sin.map((add_attributes), stream_name="CircularAverage", raw=True)

    sout = s2.map(circavg_from_calibration)
    return sin, sout


def circavg_from_calibration(image, calibration, mask=None, bins=None):
    # print("circavg : qmap : {} ".format(calibration.q_map))
    # print("circavg : rmap: {} ".format(calibration.r_map))
    return circavg(image, q_map=calibration.q_map, r_map=calibration.r_map,
                   mask=mask, bins=bins)


def circavg(image, q_map=None, r_map=None,  bins=None, mask=None, **kwargs):
    ''' computes the circular average.'''
    from skbeam.core.accumulators.binned_statistic import BinnedStatistic1D

    # figure out bins if necessary
    if bins is None:
        # guess q pixel bins from r_map
        if r_map is not None:
            # choose 1 pixel bins (roughly, not true at very high angles)
            # print("rmap not none, mask shape : {}, rmap shape :
            # {}".format(mask.shape, r_map.shape))
            # TODO : could avoid creating a mask to save time
            if mask is None:
                mask = np.ones_like(image)
            pxlst = np.where(mask == 1)
            nobins = int(np.max(r_map[pxlst]) - np.min(r_map[pxlst]) + 1)
            # print("rmap is not none, decided on {} bins".format(nobins))
        else:
            # crude guess, I'll be off by a factor between 1-sqrt(2) or so
            # (we'll have that factor less bins than we should)
            # arbitrary number
            nobins = int(np.maximum(*(image.shape))//4)

        # here we assume the rbins uniform
        bins = nobins
        # rbinstat = RadialBinnedStatistic(image.shape, bins=nobins,
        # rpix=r_map, statistic='mean', mask=mask)
        rbinstat = BinnedStatistic1D(r_map.reshape(-1), statistic='mean',
                                     bins=nobins, mask=mask.ravel())
        bin_centers = rbinstat(q_map.ravel())
        bins = center2edge(bin_centers)

    # now we use the real rbins, taking into account Ewald curvature
    # rbinstat = RadialBinnedStatistic(image.shape, bins=bins, rpix=q_map,
    # statistic='mean', mask=mask)
    rbinstat = BinnedStatistic1D(q_map.reshape(-1), statistic='mean',
                                 bins=bins, mask=mask.ravel())
    sqy = rbinstat(image.ravel())
    sqx = rbinstat.bin_centers
    # get the error from the shot noise only
    # NOTE : variance along ring could also be interesting but you
    # need to know the correlation length of the peaks in the rings... (if
    # there are peaks)
    rbinstat.statistic = "sum"
    noperbin = rbinstat(mask.ravel())
    sqyerr = np.sqrt(rbinstat(image.ravel()))
    sqyerr /= np.sqrt(noperbin)
    # the error is just the bin widths/2 here
    sqxerr = np.diff(rbinstat.bin_edges)/2.

    return Arguments(sqx=sqx, sqy=sqy, sqyerr=sqyerr, sqxerr=sqxerr)


def center2edge(centers, positive=True):
    ''' Transform a set of bin centers to edges
        This is useful for non-uniform bins.

        Note : for the edges, an assumption is made. They are extended to half
        the distance between the first two and last two points etc.

        positive : make sure the edges are monotonically increasing
    '''
    midpoints = (centers[:-1] + centers[1:])*.5
    dedge_left = centers[1]-centers[0]
    dedge_right = centers[-1]-centers[-2]
    left_edge = (centers[0] - dedge_left/2.).reshape(1)
    right_edge = (centers[-1] + dedge_right/2.).reshape(1)
    edges = np.concatenate((left_edge, midpoints, right_edge))
    # cleanup nans....
    w = np.where(~np.isnan(edges))
    edges = edges[w]
    if positive:
        newedges = list()
        mxedge = 0
        for edge in edges:
            if edge > mxedge:
                newedges.append(edge)
                mxedge = edge
        edges = np.array(newedges)
    return edges


def QPHIMapStream(bins=(400, 400)):
    '''
        Input :
                image
                mask

        Output :
            qphimap
    '''
    sin = Stream()
    sout = sin.select(0, 'mask', 'origin')\
        .map((add_attributes), stream_name="QPHIMapStream", raw=True)
    sout = sout.map(qphiavg, bins=bins)
    # from dask import compute
    # sout.apply(compute).apply(print)
    return sin, sout


def qphiavg(img, mask=None, bins=None, origin=None):
    ''' quick qphi average calculator.
        ignores bins for now
    '''
    # TODO : replace with method that takes qphi maps
    # TODO : also return q and phi of this...
    # print("In qphi average stream")
    from skbeam.core.accumulators.binned_statistic import RPhiBinnedStatistic
    rphibinstat = RPhiBinnedStatistic(img.shape, mask=mask, origin=origin,
                                      bins=bins)
    sqphi = rphibinstat(img)
    qs = rphibinstat.bin_centers[0]
    phis = rphibinstat.bin_centers[1]
    return Arguments(sqphi=sqphi, qs=qs, phis=phis)


def AngularCorrelatorStream():
    ''' Stream to run angular correlations.
        inputs : shape, origin, mask
    '''
    # from SciAnalysis.analyses.XSAnalysis import rdpc
    # s = Stream()
    # sout.

    # return s, sout
    return None


def prepare_correlation(shape, origin, mask, rbins=800, phibins=360,
                        method='bgest'):
    rdphicorr = rdpc.RDeltaPhiCorrelator(image.shape,  origin=origin,
                                         mask=mask, rbins=rbins,
                                         phibins=phibins)
    # print("kwargs : {}".format(kwargs))
    return rdphicorr

def angularcorrelation(rdphicorr, image):
    ''' Run the angular correlation on the angular correlation object.'''
    rdphicorr.run(image)
    return rdphicorr.rdeltaphiavg_n


def pack(*args, **kwargs):
    ''' pack arguments into one set of arguments.'''
    return args

def unpack(args):
    ''' assume input is a tuple, split into arguments.'''
    # print("Arguments : {}".format(args))
    return Arguments(*args)

def todict(kwargs):
    ''' assume input is a dictionary, split into kwargs.'''
    return Arguments(**kwargs)

from .tools import xystitch_accumulate, xystitch_result
def _xystitch_result(img_acc, mask_acc, origin_acc, stitchback_acc):
    # print("_xystitch_result, img_acc : {}".format(img_acc))
    return xystitch_result(img_acc, mask_acc, origin_acc, stitchback_acc)

def _xystitch_accumulate(prevstate, newstate):
    # print("_xystitch_accumulate, prevstate: {}".format(prevstate))
    return xystitch_accumulate(prevstate, newstate)


### Image stitching Stream
def ImageStitchingStream(return_intermediate=False):
    '''
        Image stitching
        Inputs:
            image : the image
            mask : the mask
            origin : the origin of the image

        Outputs:
            sin : source of stream
            sout

        return_intermediate : decide whether to return intermediate results or
        not

        NOTE : you should normalize images by exposure time before giving to
        this stream
    '''
    # TODO : add state. When False returned, need a reason why
    def validator(x):
        if not hasattr(x, 'kwargs'):
            return False
        kwargs = x['kwargs']
        expected = ['mask', 'origin', 'stitchback', 'image']
        for key in expected:
            if key not in kwargs:
                message = "{} not in kwargs".format(key)
                return dict(state=False, message=message)
        return True

    # TODO : remove the add_attributes part and just keep stream_name
    sin = Stream(stream_name="ImageStitch", validator=validator)
    # sin.map(lambda x : print("Beginning of stream data\n\n\n"))
    from dask import compute
    # TODO : remove compute requirement
    s2 = sin.map(add_attributes, stream_name="ImageStitch", raw=True)
    # s2.map(print,raw=True)
    # make the image, mask origin as the first three args
    # s2.map(lambda x : print("in image stitch : {}".format(x)), raw=True)
    # s3 = s2.map(lambda x : compute(x)[0]).select(('image', None), ('mask', None), ('origin', None), ('stitchback', None))
    s3 = s2.select(('image', None), ('mask', None), ('origin', None), ('stitchback', None))
    sout = s3.map(pack)
    sout = sout.accumulate(_xystitch_accumulate)
    # sout.map(lambda x : print("imagestitch sdoc before unpack : {}".format(x)),raw=True)
    sout = sout.map(unpack)
    sout = sout.map(_xystitch_result)
    sout = sout.map(todict)

    # now window the results and only output if stitchback from previous is nonzero
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

    swinout = swinout.map(getprevstitch, raw=True)
    # swinout.map(lambda x : print("End of stream data\n\n\n"))

    return sin, swinout



def ThumbStream(blur=None, crop=None, resize=None):
    ''' Thumbnail stream

        inputs :
            image (argument)

        output :
            reduced image

    '''
    sin = Stream()
    s0 = sin.map((add_attributes), stream_name="Thumb", raw=True)
    # s1 = sin.add_attributes(stream_name="ThumbStream")
    s1 = s0.map(_blur)
    s1 = s1.map(_crop)
    sout = s1.map(_resize).select((0, 'thumb'))

    return sin, sout

def _blur(img, sigma=None, **kwargs):
    if sigma is not None:
        from scipy.ndimage.filters import gaussian_filter
        img = gaussian_filter(img, sigma)
    return img

# TODO : fix
def _crop(img, crop=None, **kwargs):
    if crop is not None:
        x0, x1, y0, y1 = crop
        img = img[int(y0):int(y1), int(x0):int(x1)]
    return img

def _resize(img, resize=None, **kwargs):
    ''' Performs simple pixel binning

        resize bins by that number
            resize=2 bins 2x2 pixels
        resize must be an integer > 1 and also smaller than the image shape
    '''
    newimg = img
    if resize is not None:
        resize = int(resize)
        if resize > 1:
            # cut off edges
            newimg = np.zeros_like(img[resize-1::resize, resize-1::resize])
            newdims = np.array(newimg.shape)*resize
            img = img[:newdims[0], :newdims[1]]
            for i in range(resize):
                for j in range(resize):
                    newimg += img[i::resize, j::resize]
            newimg = newimg/resize**2
    return newimg

# TODO : add pixel procesing/thresholding threshold_pixels((2**32-1)-1) # Eiger inter-module gaps
# TODO : add thumb
