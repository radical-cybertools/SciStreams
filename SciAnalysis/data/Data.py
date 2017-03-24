#!/usr/bin/python
# -*- coding: utf-8 -*-
# vi: ts=4 sw=4
'''
:mod:`SciAnalysis.Data` - Base data objects for SciAnalysis
================================================
.. module:: SciAnalysis.Data
   :synopsis: Provides base classes for handling data
.. moduleauthor:: Dr. Kevin G. Yager <kyager@bnl.gov>
                    Brookhaven National Laboratory
'''

################################################################################
#  This code defines some baseline objects for handling data.
################################################################################
# Known Bugs:
#  N/A
################################################################################
# TODO:
#  Search for "TODO" below.
################################################################################


#import sys
import numpy as np
import pylab as plt
import matplotlib as mpl
from scipy import signal # For gaussian smoothing
from scipy import ndimage # For resize, etc.
from scipy import stats # For skew
#from scipy.optimize import leastsq
#import scipy.special

import PIL # Python Image Library (for opening PNG, etc.)

from . import tools


