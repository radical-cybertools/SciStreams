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


# import the analysis databroker
from uuid import uuid4
from dask import set_options
set_options(pure_default=True)

import hashlib
import numpy as np
from PIL import Image

from scipy import ndimage

from collections import ChainMap
from SciAnalysis.data.Singlet import Singlet

# internals
from SciAnalysis.analyses.Protocol import Protocol
from SciAnalysis.interfaces.SciResult import parse_sciresults

# Sources
from SciAnalysis.interfaces.databroker import databroker as source_databroker
from SciAnalysis.interfaces.file import file as source_file
from SciAnalysis.interfaces.xml import xml as source_xml

from SciAnalysis.interfaces.detectors import detectors2D

'''
    Notes : load should be a separate function
'''

from streams.core import Stream
# Calibration for SAXS data
# NOTE : Makes the assumption that the wrapper provides 'select' functionality
def CalibrationStream(wrapper=None, keymap_name=None, detector=None):
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
    '''
    if keymap_name is None:
        keymap_name = "cms"

    if detector is None:
        detector = 'pilatus300'

    # getting some hard-coded defaults
    keymap, defaults = _get_keymap_defaults(keymap_name)
    from dask import compute

    # the pipeline flow defined here
    source = Stream(wrapper=wrapper)
    calib = source.map(load_calib_dict, keymap=keymap, defaults=defaults)
    calib = calib.map(load_from_calib_dict, detector=detector, calib_defaults=defaults)

    q_maps = calib.map(_generate_qxyz_maps)
    #qx_map, qy_map, qz_map, qr_map = q_maps.multiplex(4)
    # just select first 3 args
    # TODO : add to  FAQ "Integer tuple pairs not accepted" when giving (0,1,2)
    # for ex instaead of [0,1,2]
    q_map = q_maps.select([0,1,2]).map(_generate_q_map)
    angle_map = calib.map(_generate_angle_map)
    origin = calib.map(get_beam_center)

    # make final qmap stream
    q_maps = q_maps.select([(0, 'qx_map'), (1, 'qy_map'), (2, 'qz_map'), (3, 'qr_map')])
    q_maps = q_maps.merge(q_map.select( (0, 'q_map')))
    q_maps = q_maps.merge(angle_map.select( (0, 'angle_map')))

    # rename to kwargs (easier to inspect)
    calib = calib.select( (0, 'calibration'))

    # they're relative sinks
    sinks = dict(calibration=calib, q_maps=q_maps, origin=origin)
    # return sources and the endpoints
    return source, sinks

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
    print("Generating qmaps")
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


# interface side stuff
# This is the databroker version

# TODO : Add in documentation that kwargs helps document the arguments when saved
# Just helpful, that's all
# TODO : document that if kwargs are used, they need be explicitly written
# or else handle this on your own : i.e. foo(1, b=1) versus foo(1,1)
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
            source : the source stream
            sink : the output returns a dictionary of four elements:
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

    '''
    #TODO : extend file to mltiple writers?
    source = Stream(wrapper=wrapper)
    sink = source.map(circavg)
    return source, sink


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
        else:
            # crude guess, I'll be off by a factor between 1-sqrt(2) or so
            # (we'll have that factor less bins than we should)
            nobins = np.maximum(*(image.shape))

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



def pack(*args, **kwargs):
    ''' pack arguments into one set of arguments.'''
    return args

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

    '''
    sin = Stream(wrapper=wrapper)
    # make the image, mask origin as the first three args
    s2 = sin.select([('image', None), ('mask', None), ('origin', None)])
    sout = s2.map(pack).accumulate(xystitch_accumulate)
    #sout = sout.map(xystitch_result)
    return sin, sout

def xystitch_result(img_acc, mask_acc, origin_acc):
    img_acc = img_acc/mask_acc
    mask_acc = mask_acc > 0
    return dict(image=img_acc, mask=mask_acc, origin=origin_acc)

def xystitch_accumulate(prevstate, newstate):
    '''

        (assumes IMG and mask np arrays)
        prevstate : IMG, mask, (x0, y0) triplet
        nextstate : incoming IMG, mask, (x0, y0) triplet

        returns accumulated state

        NOTE : x is cols, y is rows
            where img[rows][cols]
            so shapey, shapex = img.shape

        TODO : Do we want subpixel accuracy stitches?
            (this requires interpolating pixels, maybe not
                good for shot noise limited regime)
    '''
    # unraveling arguments
    img_acc, mask_acc, origin_acc = prevstate
    if img_acc is not None:
        shape_acc = img_acc.shape

    img_next, mask_next, origin_next = newstate
    # just in case
    img_next = img_next*mask_next
    shape_next = img_next.shape

    # logic for making new state
    # initialization:
    if img_acc is None:
        img_acc = img_next.copy()
        shape_acc = img_acc.shape
        mask_acc = mask_next.copy()
        origin_acc = origin_next
        return img_acc, mask_acc, origin_acc

    # logic for main iteration component
    # NOTE : In matplotlib, bottom and top are flipped (until plotting in
    # matrix convention), this is logically consistent here but not global
    bounds_acc = _getbounds2D(origin_acc, shape_acc)
    bounds_next = _getbounds2D(origin_next, shape_next)
    # check if image will fit in stitched image
    expandby = _getexpansion2D(bounds_acc, bounds_next)
    print("need to expand by {}".format(expandby))

    img_acc = _expand2D(img_acc, expandby)
    mask_acc = _expand2D(mask_acc, expandby)
    print("New shape : {}".format(img_acc.shape))

    origin_acc = origin_acc[0] + expandby[2], origin_acc[1] + expandby[0]
    _placeimg2D(img_next, origin_next, img_acc, origin_acc)
    _placeimg2D(mask_next, origin_next, mask_acc, origin_acc)

    return img_acc, mask_acc, origin_acc

def _placeimg2D(img_source, origin_source, img_dest, origin_dest):
    ''' place source image into dest image. use the origins for
    registration.'''
    bounds_image = _getbounds2D(origin_source, img_source.shape)
    left_bound = origin_dest[1] + bounds_image[0]
    low_bound = origin_dest[0] + bounds_image[2]
    img_dest[low_bound:low_bound+img_source.shape[0],
             left_bound:left_bound+img_source.shape[1]] += img_source

def _getbounds(center, width):
    return -center, width-1-center

def _getbounds2D(origin, shape):
    # NOTE : arrays index img[y][x] but I choose this way
    # because convention is in plotting, cols (x) is x axis
    yleft, yright = _getbounds(origin[0], shape[0])
    xleft, xright = _getbounds(origin[1], shape[1])
    return [xleft, xright, yleft, yright]

def _getexpansion(bounds_acc, bounds_next):
    expandby = [0, 0]
    # image accumulator does not extend far enough left
    if bounds_acc[0] > bounds_next[0]:
        expandby[0] = bounds_acc[0] - bounds_next[0]

    # image accumulator does not extend far enough right
    if bounds_acc[1] < bounds_next[1]:
        expandby[1] = bounds_next[1] - bounds_acc[1]

    return expandby

def _getexpansion2D(bounds_acc, bounds_next):
    expandby = list()
    expandby.extend(_getexpansion(bounds_acc[0:2], bounds_next[0:2]))
    expandby.extend(_getexpansion(bounds_acc[2:4], bounds_next[2:4]))
    return expandby

def _expand2D(img, expandby):
    ''' expand image by the expansion requirements. '''
    if not any(expandby):
        return img

    dcols = expandby[0] + expandby[1]
    drows = expandby[2] + expandby[3]

    img_tmp = np.zeros((img.shape[0] + drows, img.shape[1] + dcols))
    img_tmp[expandby[2]:expandby[2]+img.shape[0], expandby[0]:expandby[0]+img.shape[1]] = img

    return img_tmp



#### Still need to be streams

class Thumbnail(Protocol):
    ''' Compute a thumb
    '''
    def run(image=None, mask=None, blur=None, crop=None, resize=None, type='linear', vmin=None, vmax=None):
        '''
            type :
                linear : don't do anything to image
                log : take log
        '''
        img = image
        if mask is not None:
            img = img*mask
        # ensure it's delayed
        #img = delayed(img, pure=True).get()
        if blur is not None:
            img = _blur(img, blur)
        if crop is not None:
            img = _crop(img, crop)
        if resize is not None:
            img = _resize(img, resize)
        if type == 'log':
            logimg = True
        else:
            logimg = False
        if vmin is None or vmax is None:
            # TODO :make sure hist not empty?
            hist, bin_edges = np.histogram(image)
            cts = np.cumsum(hist)/np.sum(hist)
            if vmin is None:
                wstart,  = np.where(cts > .01)
                if len(wstart) > 0:
                    vmin = bin_edges[wstart[0]]
                else:
                    vmin = np.min(img)
            if vmax is None:
                wend, = np.where(cts < .99)
                if len(wend) > 0:
                    wend = wend[len(wend)-1]
                    vmax = bin_edges[wend]
                else:
                    vmax = np.max(img)
                    
        # bytescale the image
        img = _normalize(img, vmin, vmax, logimg=logimg)
        return dict(thumb=img)

# now normalize each image
def _normalize(img, mn, mx, logimg=True):
    ''' normalize to a uint8 
        This is also known as byte scaling.
    '''
    dynamic_range = 2**8-1
    if logimg:
        img = np.log10(img)
        w = np.where(np.isinf(img))
        img[w] = 0
        mn = np.log10(mn)
        mx = np.log10(mx)
    img = img-mn
    img /= (mx-mn)
    img *= dynamic_range
    img = _threshold_max(_threshold_min(img, 0), dynamic_range)#np.minimum(np.maximum(img, 0), dynamic_range)
    img = img.astype(np.uint8)
    return img

def _threshold_min(a, val):
    ''' threshold a with min val val.'''
    #subtract, then make negative zero
    a = a - val
    # negative values should be zero
    a = (a + np.abs(a))//2
    a = a + val
    return a

def _threshold_max(a, val):
    ''' threshold a with max val val.'''
    #subtract, then make negative zero
    a = val - a
    # negative values should be zero
    a = (a + np.abs(a))//2
    a = val - a
    return a


from scipy.ndimage.filters import gaussian_filter
def _blur(img, sigma):
    img = gaussian_filter(img, sigma)
    return img

# TODO : fix
def _crop(img, crop):
    x0, x1, y0, y1 = crop
    img = img[y0:y1, x0:x1]
    return img

# TODO :implement
def _resize(img, resize):
    return img

# TODO : add pixel procesing/thresholding threshold_pixels((2**32-1)-1) # Eiger inter-module gaps
# TODO : add thumb
