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

################################################################################
#  This code defines some baseline objects for x-ray analysis.
################################################################################
# Known Bugs:
#  N/A
################################################################################
# TODO:
#  Search for "TODO" below.
################################################################################



#import sys
import re # Regular expressions
from dask import delayed

import numpy as np
import pylab as plt
import matplotlib as mpl
#from scipy.optimize import leastsq
#import scipy.special

import PIL # Python Image Library (for opening PNG, etc.)
from SciAnalysis.interfaces.SciResult import parse_sciresults
from SciAnalysis.interfaces.file import reading as source_file

from scipy.interpolate import RegularGridInterpolator
from dask.base import tokenize
from functools import partial



# TODO : Check if inheritance of purity accepted to dask
# If not, we'll have to implement this on our own in a more complex way.
# I'm using pure_default for now
# pure = True means you can safely assume this function is pure
@delayed(pure_default=True)
class MasterMask:
    ''' A  master mask.'''
    def __init__(self, master=None, blemish = None, origin=None, datafile=None, **kwargs):
        from SciAnalysis.interfaces.file import reading as source_file
        ''' Either takes a data file or the master mask and origin 
            as arguments.'''
        self.blemish = blemish

        if master is not None:
            if origin is None:
                raise ValueError("Error master is specified but not origin")
            self.master_mask = master
            self.origin = origin

        elif datafile is not None:
            # gets a SciResult
            res = source_file.FileDesc(datafile).get_raw()
            # gets each element of SciResult
            self.master_mask = res["master_mask"]
            self.x0_master = res["x0_master"]
            self.y0_master = res["y0_master"]
            # rows, cols
            self.origin = self.y0_master, self.x0_master

    #@parse_sciresults
    def generate(self, shape=None, origin=None, **kwargs):
        if shape is None or origin is None:
            raise ValueError("Need to specify a shape and origin")
        mask = self.make_submask(self.master_mask, self.origin, shape=shape, origin=origin)
        # TODO check for shape and make shape optional
        if self.blemish is not None:
            mask = mask*self.blemish
        return mask

    
    #@parse_sciresults
    def make_submask(self, master_mask, master_cen, shape=None, origin=None, **kwargs):
        ''' Make a submask from the master mask,
            knowing the master_cen center and the outgoing image
            shape and center subimg_cen.
        '''
        if shape is None or origin is None:
            raise ValueError("Error, shape or origin cannot be None")
        x_master = np.arange(master_mask.shape[1]) - master_cen[1]
        y_master = np.arange(master_mask.shape[0]) - master_cen[0]
    
        interpolator = RegularGridInterpolator((y_master, x_master), master_mask)  #, bounds_error=False, fill_value=1)
    
        # make submask
        x = np.arange(shape[1]) - origin[1]
        y = np.arange(shape[0]) - origin[0]
        X, Y = np.meshgrid(x, y)
        points = (Y.ravel(), X.ravel())
        # it's a linear interpolator, so we just cast to ints (non-border regions should just be 1)
        submask = interpolator(points).reshape(shape).astype(int)
    
        return submask
