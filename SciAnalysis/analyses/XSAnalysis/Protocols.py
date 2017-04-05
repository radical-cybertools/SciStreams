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

import hashlib

from SciAnalysis.config import delayed
from scipy import ndimage

from SciAnalysis.analyses.Protocol import Protocol, run_default
from SciAnalysis.interfaces.databroker import databroker as source_databroker
from SciAnalysis.interfaces.SciResult import parse_sciresults
from SciAnalysis.interfaces.file import file as source_file
from SciAnalysis.interfaces.xml import xml as source_xml

from SciAnalysis.interfaces.detectors import detectors2D

import numpy as np
from PIL import Image

################## deprecating....
#from .Data import Data2DScattering
#from ..tools import *

'''
    Notes : load should be a separate function

'''


# interface side stuff
# This is the databroker version
class LoadSAXSImage(Protocol):
    ''' Loading a SAXS image.
   
        detector_key : for the header reading
    '''
    @delayed(pure=False)
    #@store_results('cms')
    @run_default
    # TODO have it get class name
    @parse_sciresults("XS:LoadSAXSImage")
    def run(infile=None, **kwargs):
        # TODO : Add a databroker interface
        if isinstance(infile, np.ndarray):
            img = infile
        elif isinstance(infile, str):
            img = np.array(Image.open(infile))
        else:
            raise ValueError("Sorry, did not understand the input argument: {}".format(infile))

        return img

class LoadMask(Protocol):
    ''' Load a mask from an array.'''
    def __init__(self, **kwargs):
        # set a default
        if 'threshold' not in kwargs:
            kwargs['threshold'] = 1
        self.kwargs = kwargs

    @delayed(pure=False)
    #@store_results('cms')
    @run_default
    # TODO have it get class name
    @parse_sciresults("XS:LoadSAXSImage")
    def run(mask=None, blemish=None, **kwargs):
        mask = mask > threshold
        if blemish is not None:
            mask = (mask*blemish).astype(np.uint8)
        return mask

class GenerateMask(Protocol):
    ''' Generate a mask from a master mask.'''
    def __init__(self, **kwargs):
        # set a default
        if 'threshold' not in kwargs:
            kwargs['threshold'] = 1
        self.kwargs = kwargs

    @delayed(pure=False)
    #@store_results('cms')
    @run_default
    # TODO have it get class name
    @parse_sciresults("XS:LoadSAXSImage")
    def run(master=None, master_origin=None, shape=None, blemish=None, **kwargs):
        mask = mask > threshold
        if blemish is not None:
            mask = (mask*blemish).astype(np.uint8)
        return mask



class LoadCalibration(Protocol):
    '''
        Loading a calibration, two step process:

        calib_protocol = load_cms_calibration()
        calib_protocol.add('beamx0', 50, 'pixel')
        calib_protocol.add('beamy0', 50, 'pixel')

        calib = calib_protocol(energy=13.5)

        Notes
        -----
            1. Arguments can be added either with 'add' or during function call
            2. If using dask, need to run calib.compute() to obtain result
    '''
    # defaults of function
    _defaults= {'wavelength' : {'value' : None, 'unit' : 'Angstrom'},
                 'beamx0' : {'value' : None, 'unit' : 'pixel'},
                 'beamy0' : {'value' : None, 'unit' : 'pixel'},
                 'sample_det_distance' : {'value' : None, 'unit' : 'm'},
                # Area detector specific entries:
                 # width is columns, height is rows
                 #'AD_width' : {'value' : None, 'unit' : 'pixel'},
                 #'AD_height' : {'value' : None, 'unit' : 'pixel'},
                 'pixel_size_x' : {'value' : None, 'unit' : 'pixel'},
                 'pixel_size_y' : {'value' : None, 'unit' : 'pixel'},
                  #TODO : This assumes data has this detector, not good to use, remove eventually
                 'detectors' : {'value' : ['pilatus300'], 'unit' : None},
        }
    def __init__(self, **kwargs):
        super(LoadCalibration, self).__init__(**kwargs)
        self.kwargs['calib_defaults'] = self._defaults
        self.set_keymap("cms")

    def add(self, name=None, value=None, unit=None):
        self.kwargs.update({name : {'value' : value, 'unit' : unit}})

    def set_keymap(self, name):
        if name == "cms":
            self.kwargs['calib_keymap'] = {'wavelength' : {'key' : 'calibration_wavelength_A',
                                        'unit' : 'Angstrom'},
                        'detectors' : {'key' : 'detectors',
                                        'unit' : 'N/A'},
                        'beamx0' : {'key' : 'detector_SAXS_x0_pix', 
                                    'unit' : 'pixel'},
                        'beamy0' : {'key' : 'detector_SAXS_y0_pix',
                                    'unit' : 'pixel'},
                        'sample_det_distance' : {'key' : 'detector_SAXS_distance_m',
                                                 'unit' : 'pixel'}
            }
        elif name == "None":
            self.kwargs['calib_keymap'] = {'wavelength' : {'key' : 'wavelength',
                                        'unit' : 'Angstrom'},
                        'detectors' : {'key' : 'detectors',
                                        'unit' : 'N/A'},
                        'beamx0' : {'key' : 'beamx0', 
                                    'unit' : 'pixel'},
                        'beamy0' : {'key' : 'beamy0',
                                    'unit' : 'pixel'},
                        'sample_det_distance' : {'key' : 'sample_det_distance',
                                                 'unit' : 'pixel'}
            }
        else:
            raise ValueError("Error, cannot find keymap for loading calibration")


    # this is an unbound method
    @delayed(pure=True)
    @source_databroker.store_results('cms')
    @run_default
    @parse_sciresults("XS:calibration")
    def run(calibration={}, **kwargs):
        '''
            Load calibration data from a SciResult's attributes.

            The data must be a dictionary.
            either:
                load_calibration(calibration=myCalib)
            or:
                load_calibration(wavelength=dict(value=13.5, unit='keV')) etc
            
            This is area detector specific.
        '''
        calib_keymap = kwargs['calib_keymap']
        calib_defaults = kwargs['calib_defaults']
    
        # a map from Header start doc to data
        # TODO : move out of function

        calibration = calibration.copy()
        # update calibration with all keyword arguments
        # fill in the defaults
        for key, val in calib_defaults.items():
            if key not in calibration:
                calibration[key] = val
        # now override with kwargs
        for key, val in kwargs.items():
            calibration[key] = val

        calib_tmp = dict()
        # walk through defaults
        for key in calib_defaults.keys():
            if key in calib_keymap:
                entry = calib_keymap[key]
                start_key = entry['key'] # get name of key
            else:
                entry = calib_defaults[key]
                start_key = key # get name of key

            unit = entry['unit']
            val = calibration.get(start_key, calib_defaults[key])
            # it can be a number (like from header) or a dict, check
            if isinstance(val, dict) and 'value' in val:
                val = val['value']
            calib_tmp[key] ={'value' : val, 'unit' : unit}

        # finally, get the width and height by looking at first detector in header
        # TODO : add ability to read more than one detector, maybe in calib_keymap
        if isinstance(calibration[calib_keymap['detectors']['key']], dict):
            first_detector = calibration[calib_keymap['detectors']['key']]['value'][0]
        else:
            first_detector = calibration[calib_keymap['detectors']['key']][0]

        detector_key = detectors2D[first_detector]['image_key']['value']

        # look up in local library
        pixel_size_x = detectors2D[first_detector]['pixel_size_x']['value']
        pixel_size_x_unit = detectors2D[first_detector]['pixel_size_x']['unit']
        pixel_size_y = detectors2D[first_detector]['pixel_size_y']['value']
        pixel_size_y_unit = detectors2D[first_detector]['pixel_size_y']['unit']

        img_shape = detectors2D[first_detector]['shape']

        calib_tmp['pixel_size_x'] = dict(value=pixel_size_x, unit=pixel_size_x_unit)
        calib_tmp['pixel_size_y'] = dict(value=pixel_size_y, unit=pixel_size_y_unit)
        calib_tmp['shape'] = img_shape.copy() #WARNING : copies only first level, this is one level dict
        calibration = calib_tmp
        
    
        return dict(calibration=calibration)
        

class CircularAverage(Protocol):
    ''' Circular average.'''

    @delayed(pure=True)
    @source_databroker.store_results('cms:analysis', {'sqx' : 'npy', 'sqy' : 'npy'})
    @source_xml.store_results
    @source_file.store_results
    @run_default
    @parse_sciresults("XS:CircularAverage")
    def run(image=None, calibration=None, bins=100, mask=None, **kwargs):
        #print(calibration)
        #print("computing")
        # TODO : remove when switched to mongodb
        # This is an sqlite problem
        if isinstance(calibration, str):
            import json
            calibration = json.loads(calibration)
        x0, y0 = calibration['beamx0']['value'], calibration['beamy0']['value']
        from skbeam.core.accumulators.binned_statistic import RadialBinnedStatistic
        img_shape = tuple(calibration['shape']['value'])
        #print(img_shape)
        rbinstat = RadialBinnedStatistic(img_shape, bins=bins, origin=(y0,x0), mask=mask)
        sq = rbinstat(image)
        sqx = rbinstat.bin_centers

        return dict(sqx=sqx, sqy=sq)

# TODO : add pixel procesing/thresholding threshold_pixels((2**32-1)-1) # Eiger inter-module gaps
# TODO : add thumb
