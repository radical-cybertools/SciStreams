#!/usr/bin/python
# -*- coding: utf-8 -*-
# vi: ts=4 sw=4
'''
:mod:`SciAnalysis.XSAnalysis.Data` - Base objects for XSAnalysis
================================================
.. module:: SciAnalysis.XSAnalysis
   :synopsis: Provides base classes for doing analysis of x-ray scattering data
.. moduleauthor:: Dr. Kevin G. Yager <kyager@bnl.gov>
                    Brookhaven National Laboratory
'''

###############################################################################
#  This code defines some baseline objects for x-ray analysis.
###############################################################################
# Known Bugs:
#  N/A
###############################################################################
# TODO:
#  Search for "TODO" below.
###############################################################################

from SciAnalysis.config import delayed

from SciAnalysis.data.Singlet import Singlet
from collections import ChainMap

import numpy as np

from SciAnalysis.interfaces.SciResult import parse_sciresults
from SciAnalysis.interfaces.detectors import detectors2D

from scipy.interpolate import RegularGridInterpolator
'''
    def run_default(protocol_name, xml=True, file=True, databroker=True, delay=
    True, xml_options=None, file_options=None, databroker_options=None):
'''
# TODO : need to handle classes better (figure out if it's a class, and parse
# sciresults properly)

# @run_default("XSAnalysis_Mask", False, False, False, True)


@delayed(pure=True)
class Mask:
    def __init__(self, mask=None, shape=None):
        ''' just saves a mask in object based on shape.
            Must be an array.
        '''
        if mask is None and shape is None:
            raise ValueError("Error, mask and shape both none")
        if mask is not None:
            self.mask = mask
        else:
            self.mask = np.ones(shape)


# @run_default("XSAnalysis_MasterMask", False, False, False, True)
# Normally, this would inherit mask but we can't because it's not working with
# delayed so I re-write instantiation
@delayed(pure=True)
class MasterMask:
    def __init__(self, master=None, origin=None, shape=None):
        ''' MasterMask object.
            This is meant to store a larger mask of the experimental setup,
            from where the mask for a detector is generated from.

            Parameters
            ----------
            master : 2d np.ndarray,
                the master mask

            origin : 2d np.ndarray,
                the origin of the master mask

            shape : 2D np.ndarray, optional
                the shape of the mask
        '''
        mask = master
        if mask is None and shape is None:
            raise ValueError("Error, mask and shape both none")
        if mask is not None:
            self.mask = mask
        else:
            self.mask = np.ones(shape)
        self.origin = origin


# TODO add decorators for sciresults
# for now, can't make into a sciresult
# @run_default("XSAnalysis_MaskGenerator", False, False, False, True)
class MaskGenerator:
    ''' A  master mask.'''
    def __init__(self, master, blemish, usermask=None, **kwargs):
        ''' Generate mask from known master mask.

            Take in a Master Mask object with the detector blemish and optional
            usermask.

            Parameters
            ----------

            master : a MasterMask object
                can be a dict with 'mask' and 'origin' members or
                a MasterMask object

            blemish : np.ndarray or Mask object
                a Mask object specifyin the detector mask

            user : np.ndarray or Mask object, optional
                a Mask object specifying the user mask

            Note
            ----

            This is meant for SAXS detectors that do not tilt. For SAXS
            detectors with tilting, or WAXS detectors, a different method may
            be necessary.

            Examples
            --------

            mm = MasterMask(master, blemish, origin)
            # y0 is rows, x0 is columns
            mask = mm.generate((y0,x0))
        '''
        self.load_master(master)
        self.load_blemish(blemish)
        self.load_usermask(usermask)

    def load_master(self, master):
        self.mastermask = master.mask
        self.masterorigin = master.origin

    def load_blemish(self, blemish):
        try:
            self.blemish = blemish.mask
        except AttributeError:
            self.blemish = blemish

    def load_usermask(self, usermask):
        try:
            self.usermask = usermask.mask
        except AttributeError:
            self.usermask = usermask

    def generate(self, origin=None, **kwargs):
        if origin is None:
            raise ValueError("Need to specify an origin")
        # Note this returns a sciresult object
        mask = make_submask(self.mastermask, self.masterorigin,
                            shape=self.blemish.shape, origin=origin,
                            blemish=self.blemish)
        return mask


@delayed(pure=True)
@parse_sciresults("XSAnalysis:Data:make_submask")
def make_submask(master_mask, master_cen, shape=None, origin=None,
                 blemish=None):
    ''' Make a submask from the master mask,
        knowing the master_cen center and the outgoing image
        shape and center subimg_cen.
    '''
    if shape is None or origin is None:
        raise ValueError("Error, shape or origin cannot be None")
    x_master = np.arange(master_mask.shape[1]) - master_cen[1]
    y_master = np.arange(master_mask.shape[0]) - master_cen[0]

    interpolator = RegularGridInterpolator((y_master, x_master), master_mask)

    # make submask
    x = np.arange(shape[1]) - origin[1]
    y = np.arange(shape[0]) - origin[0]
    X, Y = np.meshgrid(x, y)
    points = (Y.ravel(), X.ravel())
    # it's a linear interpolator, so we just cast to ints (non-border regions
    # should just be 1)
    submask = interpolator(points).reshape(shape).astype(int)
    if blemish is not None:
        submask = submask*blemish

    return submask


# Calibration for SAXS data
# @delayed(pure=True)
class Calibration:
    '''
        Stores aspects of the experimental setup; especially the calibration
        parameters for a particular detector. That is, the wavelength, detector
        distance, and pixel size that are needed to convert pixel (x,y) into
        reciprocal-space (q) value.

        This class may also store other information about the experimental
        setup (such as beam size and beam divergence).

        This will load from various sources:
            - dict
            - databroker Header
    '''
    # defaults of function
    # TODO : add error in value
    def __init__(self, attributes=None, detector='pilatus300'):
        '''
            Need to specify a header *and* detector.
            If not, does not initialize data.

            This object involves two data sets:
                1. the experiment
                2. the detector form the detector database
            The object's member functions should now return delayed instances.
            The object is just a way to book-keep complex computations.
            Making delayed instances ensures results are saved in the cache.
        '''
        # TODO : can be updated to more keymaps
        # TODO : allow two-step definition
        self.set_keymap("cms")
        if attributes is not None and detector is not None:
            # compute the first since it's performed on random data
            calib_dict = load_calib_dict(attributes, self.calib_keymap,
                                         self.calib_defaults)\
                                         .compute()('calib_dict').get()
            calib = self.calib_defaults
            self.calibration = load_from_calib_dict(calib_dict, detector,
                                                    calib)('calibration')
        else:
            self.calibration = None

        # this defines the delayed computations to be run
        # These are all delayed so it's fine to access them this way
        # NOTE : assumes it's a SciResult object
        self.qx_map = _generate_qxyz_maps(self.calibration)('qx_map')
        self.qy_map = _generate_qxyz_maps(self.calibration)('qy_map')
        self.qz_map = _generate_qxyz_maps(self.calibration)('qz_map')
        self.qr_map = _generate_qxyz_maps(self.calibration)('qr_map')
        self.r_map = _generate_r_map(self.calibration)
        self.q_map = _generate_q_map(self.calibration, self.qx_map,
                                     self.qy_map, self.qz_map)
        self.angle_map = _generate_angle_map(self.calibration)

    def add(self, name=None, value=None, unit=None):
        self.update(**Singlet(name,  value, unit))

    def set_keymap(self, name):
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
        self.calib_keymap = keymap
        self.calib_defaults = defaults


@delayed(pure=True)
@parse_sciresults("XSAnalysis:Data:load_calib_dict")
def load_calib_dict(attributes, calib_keymap, calib_defaults):
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

    return dict(calib_dict=newdict)


@delayed(pure=True)
@parse_sciresults("XSAnalysis:Data:load_from_header")
def load_from_calib_dict(calib_dict, detector, calib_defaults):
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

    return dict(calibration=calibration)


# when delayed no longer depends on self
@delayed(pure=True)
@parse_sciresults("XSAnalysis:Data:generate_qxyz_maps")
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
    return dict(qx_map=qx_map_data, qy_map=qy_map_data,
                qz_map=qz_map_data, qr_map=qr_map_data)


@delayed(pure=True)
@parse_sciresults("XSAnalysis:Data:generate_r_map")
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


@delayed(pure=True)
@parse_sciresults("XSAnalysis:Data:generate_q_map")
def _generate_q_map(calibration, qx_map, qy_map, qz_map):
    '''Returns a 2D map of the q-value associated with each pixel position
    in the detector image.
    Note : assumes square pixels
    '''
    q_map = np.sqrt(qx_map**2 + qy_map**2 + qz_map**2)

    return q_map


@delayed(pure=True)
@parse_sciresults("XSAnalysis:Data:generate_angle_map")
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


class TranslationMotor(object):
    ''' A virtual translation motor for the detector.'''
    pass


class Image2D:
    pass


class Sample(object):
    ''' A sample. '''
    def __init__(self, **kwargs):
        self.orientation = np.identity(3)


class Detector2D(object):
    ''' A 2D detector object. Contains detector information.'''
    _required = ['shape']

    def __init__(self, **kwargs):
        self.orientation = np.identity(3)


class XrayBeam(object):
    ''' The Xray beam.'''
    _required = ['wavelength']

    def __init__(self, **kwargs):
        for elem in self._required:
            if elem not in kwargs:
                raise ValueError("{} not supplied".format(elem))

        for key, val in kwargs.items():
            setattr(self, key, val)
        # now pass on kwargs
        super(XrayBeam, self).__init__(**kwargs)


class Spectrometer2D(Detector2D, XrayBeam):
    ''' A combination of Detector2D + xray.'''
    def __init__(self, **kwargs):
        ''' Initialize with a detector, the xray beam and beam_center.'''
        super(Spectrometer2D, self).__init__(self, **kwargs)

# class StitchedImage(Image2D, Spectrometer2D, Mask):
# pass
