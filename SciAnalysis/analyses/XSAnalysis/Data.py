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

from SciAnalysis.analyses.XSAnalysis.tools import xystitch_accumulate

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
