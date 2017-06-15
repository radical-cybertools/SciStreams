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


    Stream version of data
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

from SciAnalysis.data.Singlet import Singlet
from collections import ChainMap

import numpy as np

from SciAnalysis.interfaces.detectors import detectors2D

from scipy.interpolate import RegularGridInterpolator
from scipy.ndimage.interpolation import rotate as scipy_rotate

from SciAnalysis.analyses.XSAnalysis.tools import xystitch_accumulate, roundbydigits

from dask.delayed import tokenize
'''
    def run_default(protocol_name, xml=True, file=True, databroker=True, delay=
    True, xml_options=None, file_options=None, databroker_options=None):
'''
# TODO : need to handle classes better (figure out if it's a class, and parse
# sciresults properly)

# @run_default("XSAnalysis_Mask", False, False, False, True)


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
                origin is in row,col format (x,y)

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
    def __init__(self, obstruction, blemish, usermask=None, **kwargs):
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
        self.mastermask = obstruction.mask
        self.masterorigin = obstruction.origin
        self.load_blemish(blemish)
        self.load_usermask(usermask)

    def rotate_obstruction(self, phi):
        ''' Rotate obstruction in degrees.'''
        # TODO : Add rotate about origin
        pass


    def load_obstruction(self, obstruction):
        self.mastermask = obstruction.mask
        self.masterorigin = obstruction.origin

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


def make_submask(master_mask, master_cen, shape=None, origin=None,
                 blemish=None):
    ''' Make a submask from the master mask,
        knowing the master_cen center and the outgoing image
        shape and center subimg_cen.

        origin is in row,col format (x,y)
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

    return submask*(submask > 0.5)


class Obstruction:
    ''' General obstruction on a detector. This is used to generate a mask.
    Origin is the origin of the absolute coordinate system that all
    obstructions should align to.

    NOTE : An obstruction is defined 1 where it obstructs and 0 otherwise. This
    is opposite of mask.
    NOTE #2 : adding and subtracting these can results in larger arrays holding
    the obstruction
    NOTE #3 : this assumes binary images (and uses _thresh for threshold)

    NOTE #4 : the mask property of this object cannot be edited

    image : image of the obstruction : 1 is present, 0 absent
    origin : the origin of the obstruction


    '''
    _thresh = .5
    def __init__(self, mask, origin):
        # invert image
        self.image = (mask < 1).astype(int)
        self.origin = origin

    @property
    def mask(self):
        return (self.image < 1).astype(int)

    def __add__(self, newob):
        ''' Stitch the two together. Create a new obstruction object from
        this.

            Patched a bit so I can reuse stitching method.
        '''
        prevstate = self.image.copy(), np.ones_like(self.image), self.origin, 1
        nextstate = newob.image, np.ones_like(newob.image), newob.origin, 1
        newstate = xystitch_accumulate(prevstate, nextstate)

        image, mask, origin, stitch = newstate

        # less than because obstruction expects a mask, not image (image has 1
        # where obsstruction present)
        retobj = Obstruction((image < self._thresh).astype(int), origin)

        return retobj

    def __sub__(self, newob):
        ''' Stitch the two together. Create a new obstruction object from
        this'''
        prevstate = self.image.copy(), np.ones_like(self.image), self.origin, 1
        nextstate = -1*newob.image, np.ones_like(newob.image), newob.origin, 1
        newstate = xystitch_accumulate(prevstate, nextstate)
        img, mask, origin, stitch = newstate

        # less than because obstruction expects a mask, not image (image has 1
        # where obsstruction present)
        retobj = Obstruction((img < self._thresh).astype(int), origin)

        return retobj

    def rotate(self, phi, origin=None):
        ''' rotate the obstruction in phi, in degrees.'''
        # re-center image (for scipy rotate)
        image = self.image
        old_origin = self.origin
        if origin is None:
            origin = old_origin
            dorigin = (0,0)
        else:
            dorigin = old_origin[0] - origin[0], old_origin[1] - origin[1]
        # re-center
        image, origin = self._center(image, origin)
        rotimg = scipy_rotate(self.image, phi, reshape=True)

        # get back to original origin
        origin = origin[0] + dorigin[0], origin[1] + dorigin[1]

        return Obstruction((rotimg > self._thresh).astype(int), origin)

    def _center(self, img, origin):
        ''' center an image to array center.'''
        # center an image to origin
        # find largest dimension first
        dimx = 2*np.max([origin[1], img.shape[1]-origin[1]-1])+1
        dimy = 2*np.max([origin[0], img.shape[0]-origin[0]-1])+1
        # make new array with these dimensions
        newimg = np.zeros((dimy, dimx))

        cen = newimg.shape[0]//2, newimg.shape[1]//2
        y0 = cen[0]-origin[0]
        x0 = cen[1]-origin[1]
        newimg[y0:y0+img.shape[0], x0:x0+img.shape[1]] = img
        return newimg, cen



########## Work in progress #################


# Calibration
################################################################################
class Calibration(object):
    '''Stores aspects of the experimental setup; especially the calibration
    parameters for a particular detector. That is, the wavelength, detector
    distance, and pixel size that are needed to convert pixel (x,y) into
    reciprocal-space (q) value.

    This class may also store other information about the experimental setup
    (such as beam size and beam divergence).
    '''

    def __init__(self, wavelength_A=None, distance_m=None, pixel_size_um=None,
                 width=None, height=None, x0=None, y0=None):

        self.wavelength_A = wavelength_A
        self.distance_m = distance_m
        self.pixel_size_um = pixel_size_um
        # TODO :should we force these to be defined?
        # or add another object inheritance layer?
        if width is None:
            self.width = None
        else:
            self.width = width

        if height is None:
            self.height = None
        else:
            self.height = height

        if x0 is None:
            self.x0 = None
        else:
            self.x0 = x0

        if y0 is None:
            self.y0 = None
        else:
            self.y0 = y0


        # Data structures will be generated as needed
        # (and preserved to speedup repeated calculations)
        self.clear_maps()



    # Experimental parameters
    ########################################

    def set_wavelength(self, wavelength_A):
        '''Set the experimental x-ray wavelength (in Angstroms).'''

        self.wavelength_A = wavelength_A


    def get_wavelength(self):
        '''Get the x-ray beam wavelength (in Angstroms) for this setup.'''

        return self.wavelength_A


    def set_energy(self, energy_keV):
        '''Set the experimental x-ray beam energy (in keV).'''

        energy_eV = energy_keV*1000.0
        energy_J = energy_eV/6.24150974e18

        h = 6.626068e-34 # m^2 kg / s
        c = 299792458 # m/s

        wavelength_m = (h*c)/energy_J
        self.wavelength_A = wavelength_m*1e+10


    def get_energy(self):
        '''Get the x-ray beam energy (in keV) for this setup.'''

        h = 6.626068e-34 # m^2 kg / s
        c = 299792458 # m/s

        wavelength_m = self.wavelength_A*1e-10 # m
        E = h*c/wavelength_m # Joules

        E *= 6.24150974e18 # electron volts

        E /= 1000.0 # keV

        return E


    def get_k(self):
        '''Get k = 2*pi/lambda for this setup, in units of inverse Angstroms.'''

        return 2.0*np.pi/self.wavelength_A


    def set_distance(self, distance_m):
        '''Sets the experimental detector distance (in meters).'''

        self.distance_m = distance_m


    def set_pixel_size(self, pixel_size_um=None, width_mm=None, num_pixels=None):
        '''Sets the pixel size (in microns) for the detector. Pixels are assumed
        to be square.'''

        if pixel_size_um is not None:
            self.pixel_size_um = pixel_size_um

        else:
            if num_pixels is None:
                num_pixels = self.width
            pixel_size_mm = width_mm*1./num_pixels
            self.pixel_size_um = pixel_size_mm*1000.0


    def set_beam_position(self, x0, y0):
        '''Sets the direct beam position in the detector images (in pixel
        coordinates).'''

        self.x0 = x0
        self.y0 = y0

    @property
    def origin(self):
        return self.x0, self.y0


    def set_image_size(self, width, height=None):
        '''Sets the size of the detector image, in pixels.'''

        self.width = width
        if height is None:
            # Assume a square detector
            self.height = width
        else:
            self.height = height


    @property
    def q_per_pixel(self):
        '''Gets the delta-q associated with a single pixel. This is computed in
        the small-angle limit, so it should only be considered approximate.
        For instance, wide-angle detectors will have different delta-q across
        the detector face.'''

        if not hasattr(self, '_q_per_pixel') or self._q_per_pixel is not None:
            return self._q_per_pixel

        c = (self.pixel_size_um/1e6)/self.distance_m
        twotheta = np.arctan(c) # radians

        self._q_per_pixel = 2.0*self.get_k()*np.sin(twotheta/2.0)

        return self._q_per_pixel


    # Maps
    ########################################

    def clear_maps(self):
        self.r_map_data = None
        self._q_per_pixel = None
        self.q_map_data = None
        self.angle_map_data = None

        self.qx_map_data = None
        self.qy_map_data = None
        self.qz_map_data = None
        self.qr_map_data = None


    @property
    def r_map(self):
        '''Returns a 2D map of the distance from the origin (in pixel units) for
        each pixel position in the detector image.'''

        if self.r_map_data is not None:
            return self.r_map_data

        x = np.arange(self.width) - self.x0
        y = np.arange(self.height) - self.y0
        X, Y = np.meshgrid(x, y)
        R = np.sqrt(X**2 + Y**2)

        self.r_map_data = R

        return self.r_map_data


    def q_map(self):
        '''Returns a 2D map of the q-value associated with each pixel position
        in the detector image.'''

        if self.q_map_data is not None:
            return self.q_map_data

        c = (self.pixel_size_um/1e6)/self.distance_m
        twotheta = np.arctan(self.r_map()*c) # radians

        self.q_map_data = 2.0*self.get_k()*np.sin(twotheta/2.0)

        return self.q_map_data


    def angle_map(self):
        '''Returns a map of the angle for each pixel (w.r.t. origin).
        0 degrees is vertical, +90 degrees is right, -90 degrees is left.'''

        if self.angle_map_data is not None:
            return self.angle_map_data

        x = (np.arange(self.width) - self.x0)
        y = (np.arange(self.height) - self.y0)
        X,Y = np.meshgrid(x,y)
        #M = np.degrees(np.arctan2(Y, X))
        # Note intentional inversion of the usual (x,y) convention.
        # This is so that 0 degrees is vertical.
        #M = np.degrees(np.arctan2(X, Y))

        # TODO: Lookup some internal parameter to determine direction
        # of normal. (This is what should befine the angle convention.)
        M = np.degrees(np.arctan2(X, -Y))


        self.angle_map_data = M

        return self.angle_map_data


    def qx_map(self):
        if self.qx_map_data is not None:
            return self.qx_map_data

        self._generate_qxyz_maps()

        return self.qx_map_data

    def qy_map(self):
        if self.qy_map_data is not None:
            return self.qy_map_data

        self._generate_qxyz_maps()

        return self.qy_map_data

    def qz_map(self):
        if self.qz_map_data is not None:
            return self.qz_map_data

        self._generate_qxyz_maps()

        return self.qz_map_data

    def qr_map(self):
        if self.qr_map_data is not None:
            return self.qr_map_data

        self._generate_qxyz_maps()

        return self.qr_map_data



    def _generate_qxyz_maps(self):
        # Conversion factor for pixel coordinates
        # (where sample-detector distance is set to d = 1)
        c = (self.pixel_size_um/1e6)/self.distance_m

        x = np.arange(self.width) - self.x0
        y = np.arange(self.height) - self.y0
        X, Y = np.meshgrid(x, y)
        R = np.sqrt(X**2 + Y**2)

        #twotheta = np.arctan(self.r_map()*c) # radians
        theta_f = np.arctan2( X*c, 1 ) # radians
        #alpha_f_prime = np.arctan2( Y*c, 1 ) # radians
        alpha_f = np.arctan2( Y*c*np.cos(theta_f), 1 ) # radians


        qpp = self.q_per_pixel
        self.qx_map_data = self.get_k()*np.sin(theta_f)*np.cos(alpha_f)
        self.qy_map_data = self.get_k()*( np.cos(theta_f)*np.cos(alpha_f) - 1 ) # TODO: Check sign
        self.qz_map_data = -1.0*self.get_k()*np.sin(alpha_f)

        self.qr_map_data = np.sign(self.qx_map_data)*np.sqrt(np.square(self.qx_map_data) + np.square(self.qy_map_data))

        self.r_map()
        self.q_map()
        self.angle_map()




    # End class Calibration(object)
    ########################################

from dask.base import normalize_token
@normalize_token.register(Calibration)
def tokenize_calibration(self):
    # function to allow for intelligent caching
    # all all computations of data and submethods
    # need to specify pure=True flag
    args = [self.wavelength_A, self.distance_m]
    args.append(self.pixel_size_um)
    if self.width is not None:
        args.append(self.width)
    if self.height is not None:
        args.append(self.height)

    # round these by 3 digits
    if self.x0 is not None:
        args.append(roundbydigits(self.x0, 3))
    if self.y0 is not None:
        args.append(roundbydigits(self.y0, 3))
    if self.angle_map_data is not None:
        args.append(roundbydigits(self.angle_map_data, 3))
    if self.q_map_data is not None:
        args.append(roundbydigits(self.q_map_data, 3))
    if self.qr_map_data is not None:
        args.append(roundbydigits(self.qr_map_data, 3))
    if self.qx_map_data is not None:
        args.append(roundbydigits(self.qx_map_data, 3))
    if self.qy_map_data is not None:
        args.append(roundbydigits(self.qy_map_data, 3))
    if self.qz_map_data is not None:
        args.append(roundbydigits(self.qz_map_data, 3))

    return normalize_token(args)



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
