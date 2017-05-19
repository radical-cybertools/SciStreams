#!/usr/bin/python
# -*- coding: utf-8 -*-
# vi: ts=4 sw=4
'''
:mod:`SciAnalysis.XSAnalysis.Protocols` - Data analysis protocols
================================================
.. module:: SciAnalysis.XSAnalysis.Protocols
   :synopsis: Convenient protocols for data analysis.
.. moduleauthor:: Dr. Kevin G. Yager <kyager@bnl.gov>
                    Brookhaven National Laboratory
'''

################################################################################
#  Data analysis protocols.
################################################################################
# Known Bugs:
#  N/A
################################################################################
# TODO:
#  Search for "TODO" below.
################################################################################

from SciAnalysis.globals import cache, client
cache.register()


# import the analysis databroker
from uuid import uuid4
from dask import set_options
set_options(pure_default=True)

import hashlib
import numpy as np
from PIL import Image

from scipy import ndimage

from dask.delayed import delayed

from collections import ChainMap
from SciAnalysis.data.Singlet import Singlet

# Sources
from SciAnalysis.interfaces.databroker import databroker as source_databroker
from SciAnalysis.interfaces.file import file as source_file
from SciAnalysis.interfaces.xml import xml as source_xml

from SciAnalysis.interfaces.detectors import detectors2D

'''
    Notes : load should be a separate function
'''

def add_attributes(sdoc, **attr):
    newsdoc = StreamDoc(sdoc)
    newsdoc.add(attributes=attr)
    return newsdoc

from SciAnalysis.interfaces.StreamDoc import Stream, StreamDoc

from collections import deque
# cache for qmaps
QMAP_CACHE = deque(maxlen=1000)

# Calibration for SAXS data
# NOTE : Makes the assumption that the wrapper provides 'select' functionality
def CalibrationStream(keymap_name=None, detector=None, wrapper=None):
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

    # the pipeline flow defined here
    sin = Stream(wrapper=wrapper)
    s2 = sin.apply(delayed(add_attributes), stream_name="Calibration")
    calib = s2.map(load_calib_dict, keymap=keymap, defaults=defaults)
    calib = calib.map(load_from_calib_dict, detector=detector, calib_defaults=defaults)

    q_maps = calib.map(_generate_qxyz_maps)
    # for distributed regime, store intermediate values
    # sink the cache for the qmaps
    q_maps.apply(client.compute).sink(QMAP_CACHE.append)
    # compute it so that it's cached on cluster
    # TODO : figure out best way to make this dask and non dask compatible
    #q_maps.apply(print)
    #q_maps.apply(compute, pure=True)
    #qx_map, qy_map, qz_map, qr_map = q_maps.multiplex(4)
    # just select first 3 args
    # TODO : add to  FAQ "Integer tuple pairs not accepted" when giving (0,1,2)
    # for ex instaead of [0,1,2]
    q_map = q_maps.select(0,1,2).map(_generate_q_map)
    angle_map = calib.map(_generate_angle_map)
    r_map = calib.map(_generate_r_map).select((0, 'r_map'))
    origin = calib.map(get_beam_center)

    # make final qmap stream
    q_maps = q_maps.select((0, 'qx_map'), (1, 'qy_map'), (2, 'qz_map'), (3, 'qr_map'))
    q_maps = q_maps.merge(q_map.select((0, 'q_map')), r_map)
    q_maps = q_maps.merge(angle_map.select((0, 'angle_map')))

    # rename to kwargs (easier to inspect)
    calib = calib.select((0, 'calibration'))

    # they're relative sinks
    sout = dict(calibration=calib, q_maps=q_maps, origin=origin)
    # return sin and the endpoints
    return sin, sout

def get_beam_center(obj):
    ''' Get beam center in row, col (y,x) format.'''
    x0 = obj['beamx0']['value']
    y0 = obj['beamy0']['value']
    return (y0, x0),

def _get_keymap_defaults(name):
    ''' Get the keymap for the calibration, along with default values.'''
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
    calib_keymap = keymap
    calib_defaults = defaults

    # TODO Allow for different units
    olddict = attributes
    newdict = dict()
    for key, newkey in calib_keymap.items():
        try:
            newdict.update(Singlet(key, olddict[newkey],
                                   calib_defaults[key]['unit']))
        except KeyError:
            raise KeyError("There is an entry missing" +
                           " in header : {}.".format(newkey) +
                           "Cannot proceed")
    # TODO : mention dicts need to be returned as tuples or encapsulated in a dict
    return newdict,


def load_from_calib_dict(calib_dict, detector=None, calib_defaults=None):
    '''
        Update calibration with all keyword arguments fill in the defaults
    '''
    calibration = calib_dict

    calib_tmp = dict()
    calib_tmp.update(calibration)

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
    h = 6.626068e-34  # m^2 kg / s
    c = 299792458  # m/s
    wavelength = calib_tmp['wavelength']['value']*1e-10  # m
    E = h*c/wavelength  # Joules
    E *= 6.24150974e18  # electron volts
    E /= 1000.0  # keV
    calib_tmp.update(Singlet('energy', E, 'keV'))
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
    calibration = calib_tmp

    return calibration,


# when delayed no longer depends on self
def _generate_qxyz_maps(calibration):
    ''' Note : assumes square pixels.'''

    # TODO : add units
    # Conversion factor for pixel coordinates
    # (where sample-detector distance is set to d = 1)
    #print("Generating qmaps")
    pixel_size = calibration['pixel_size_x']['value']
    distance = calibration['sample_det_distance']['value']
    c = (pixel_size/1e6)/distance
    shape = calibration['shape']['value']
    # rows, cols
    height, width = shape[0], shape[1]
    x0, y0 = calibration['beamx0']['value'], calibration['beamy0']['value']

    x = np.arange(width) - x0
    y = np.arange(height) - y0
    X, Y = np.meshgrid(x, y)

    # twotheta = np.arctan(self.r_map()*c) # radians
    theta_f = np.arctan2(X*c, 1)  # radians
    alpha_f = np.arctan2(Y*c*np.cos(theta_f), 1)  # radians

    kvector_mag = calibration['k']['value']
    qx_map_data = kvector_mag*np.sin(theta_f)*np.cos(alpha_f)
    # TODO: Check sign
    qy_map_data = kvector_mag*(np.cos(theta_f)*np.cos(alpha_f) - 1)
    qz_map_data = -1.0*kvector_mag*np.sin(alpha_f)

    qr_map_data = np.sqrt(np.square(qx_map_data) + np.square(qy_map_data))

    return qx_map_data, qy_map_data, qz_map_data, qr_map_data


def _generate_r_map(calibration):
    '''Returns a 2D map of the distance from the origin (in pixel units) for
    each pixel position in the detector image.'''
    # rows, cols
    height, width = calibration['shape']['value']
    x0, y0 = calibration['beamx0']['value'], calibration['beamy0']['value']

    x = np.arange(width) - x0
    y = np.arange(height) - y0
    X, Y = np.meshgrid(x, y)
    R = np.sqrt(X**2 + Y**2)

    return R


def _generate_q_map(qx_map, qy_map, qz_map):
    '''Returns a 2D map of the q-value associated with each pixel position
    in the detector image.
    calibration, qx_map, qy_map, qz_map = args
    Note : assumes square pixels
    '''
    q_map = np.sqrt(qx_map**2 + qy_map**2 + qz_map**2)

    return q_map


def _generate_angle_map(calibration):
    '''Returns a map of the angle for each pixel (w.r.t. origin).
    0 degrees is vertical, +90 degrees is right, -90 degrees is left.'''
    # rows, cols
    height, width = calibration['shape']['value']
    x0, y0 = calibration['beamx0']['value'], calibration['beamy0']['value']
    x = (np.arange(width) - x0)
    y = (np.arange(height) - y0)
    X, Y = np.meshgrid(x, y)
    # M = np.degrees(np.arctan2(Y, X))
    # Note intentional inversion of the usual (x,y) convention.
    # This is so that 0 degrees is vertical.
    # M = np.degrees(np.arctan2(X, Y))

    # TODO: Lookup some internal parameter to determine direction
    # of normal. (This is what should befine the angle convention.)
    M = np.degrees(np.arctan2(X, -Y))

    return M


def CircularAverageStream(wrapper=None):
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
    #TODO : extend file to mltiple writers?
    sin  = Stream(wrapper=wrapper)
    s2 = sin.apply(delayed(add_attributes), stream_name="CircularAverage")
    sout = s2.map(circavg)
    return sin, sout


def circavg(image, q_map=None, r_map=None,  bins=None, mask=None, **kwargs):
    ''' computes the circular average.'''
    from skbeam.core.accumulators.binned_statistic import RadialBinnedStatistic

    # figure out bins if necessary
    if bins is None:
        # guess q pixel bins from r_map
        if r_map is not None:
            # choose 1 pixel bins (roughly, not true at very high angles)
            pxlst = np.where(mask == 1)
            nobins = int(np.max(r_map[pxlst]) - np.min(r_map[pxlst]) + 1)
            print("rmap is not none, decided on {} bins".format(nobins))
        else:
            # crude guess, I'll be off by a factor between 1-sqrt(2) or so
            # (we'll have that factor less bins than we should)
            # arbitrary number
            nobins = np.maximum(*(image.shape))//4

        # here we assume the rbins uniform
        bins = nobins
        rbinstat = RadialBinnedStatistic(image.shape, bins=nobins,
                rpix=r_map, statistic='mean', mask=mask)
        bin_centers = rbinstat(q_map)
        bins = center2edge(bin_centers)


    # now we use the real rbins, taking into account Ewald curvature
    rbinstat = RadialBinnedStatistic(image.shape, bins=bins, rpix=q_map,
            statistic='mean', mask=mask)
    sqy = rbinstat(image)
    sqx = rbinstat.bin_centers
    # get the error from the shot noise only 
    # NOTE : variance along ring could also be interesting but you 
    # need to know the correlation length of the peaks in the rings... (if there are peaks)
    rbinstat.statistic = "sum"
    noperbin = rbinstat(mask)
    sqyerr = np.sqrt(rbinstat(image))
    sqyerr /= np.sqrt(noperbin)
    # the error is just the bin widths/2 here
    sqxerr = np.diff(rbinstat.bin_edges)/2.

    return dict(sqx=sqx, sqy=sqy, sqyerr=sqyerr, sqxerr=sqxerr)

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


def QPHIMapStream(wrapper=None, bins=(400,400)):
    '''
        Input :
                image
                mask

        Output :
            qphimap
    '''
    sin = Stream(wrapper=wrapper)
    sout = sin.select(0, 'mask', 'origin')\
            .apply(delayed(add_attributes), stream_name="QPHIMapStream")
    sout = sout.map(qphiavg, bins=bins)
    #from dask import compute
    #sout.apply(compute).apply(print)
    return sin, sout

def qphiavg(img, mask=None, bins=None, origin=None):
    ''' quick qphi average calculator.
        ignores bins for now
    '''
    # TODO : replace with method that takes qphi maps
    # TODO : also return q and phi of this...
    from skbeam.core.accumulators.binned_statistic import RPhiBinnedStatistic
    rphibinstat = RPhiBinnedStatistic(img.shape, mask=mask, origin=origin, bins=bins)
    sqphi = rphibinstat(img)
    qs = rphibinstat.bin_centers[0]
    phis = rphibinstat.bin_centers[1]
    return dict(sqphi=sqphi, qs=qs, phis=phis)





def pack(*args, **kwargs):
    ''' pack arguments into one set of arguments.'''
    return args

from SciAnalysis.analyses.XSAnalysis.tools import xystitch_accumulate, xystitch_result
### Image stitching Stream
def ImageStitchingStream(wrapper=None):
    '''
        Image stitching
        Inputs:
            image : the image
            mask : the mask
            origin : the origin of the image

        Outputs:
            sin : source of stream
            sout

        NOTE : you should normalize images by exposure time before giving to
        this stream

    '''
    sin = Stream(wrapper=wrapper)
    s2 = sin.apply(delayed(add_attributes), stream_name="ImageStitch")
    # make the image, mask origin as the first three args
    s3 = s2.select(('image', None), ('mask', None), ('origin', None), ('stitchback', None))
    sout = s3.map(pack).accumulate(xystitch_accumulate)

    # debugging
    #sout = sout.select([(0, 'image'), (1, 'mask'), (2, 'origin'), (3, 'stitch')])

    from dask.delayed import compute
    sout = sout.map(xystitch_result)

    #    Testing a way to control flow based on stitch param, still working on
    #            it...
    #    NOTE : stitch also expected in attribute
    #    def predicate(sdocs):
    #        # make sure it's already filled
    #        if len(sdocs) != 2:
    #            return False
    #        # look at latest doc
    #        if sdocs[1]['attributes']['stitch'] == 0:
    #            return True
    #        else:
    #            return False
    #
    #    def get_first(sdocs):
    #        print("Great! got a stitch!")
    #        # get earlier doc
    #        return sdocs[0]
    #
    #    # only output when stitch complete, something like:
    #    sout = sout.sliding_window(2)
    #    sout = sout.filter(predicate).apply(get_first)
    #    

    return sin, sout



def ThumbStream(wrapper=None, blur=None, crop=None, resize=None):
    ''' Thumbnail stream

        inputs :
            image (argument)

        output :
            reduced image

    '''
    sin = Stream(wrapper=wrapper)
    s0 = sin.apply(delayed(add_attributes), stream_name="Thumb")
    #s1 = sin.add_attributes(stream_name="ThumbStream")
    s1 = s0.map(_blur)
    s1 = s1.map(_crop)
    sout = s1.map(_resize).select((0, 'thumb'))

    return sin, sout

def _blur(img, sigma=None, **kwargs):
    if sigma is not None:
        from scipy.ndimage.filters import gaussian_filter
        img = gaussian_filter(img, sigma)
    return img,

# TODO : fix
def _crop(img, crop=None, **kwargs):
    if crop is not None:
        x0, x1, y0, y1 = crop
        img = img[y0:y1, x0:x1]
    return img,

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
    return newimg,

# TODO : add pixel procesing/thresholding threshold_pixels((2**32-1)-1) # Eiger inter-module gaps
# TODO : add thumb
