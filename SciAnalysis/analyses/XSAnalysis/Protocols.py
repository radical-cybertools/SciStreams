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

# internals
from SciAnalysis.config import delayed
from SciAnalysis.analyses.Protocol import Protocol, run_default
from SciAnalysis.interfaces.SciResult import parse_sciresults

# Sources
from SciAnalysis.interfaces.databroker import databroker as source_databroker
from SciAnalysis.interfaces.file import file as source_file
from SciAnalysis.interfaces.xml import xml as source_xml

from SciAnalysis.interfaces.detectors import detectors2D

'''
    Notes : load should be a separate function
'''


# interface side stuff
# This is the databroker version

# TODO : Add in documentation that kwargs helps document the arguments when saved
# Just helpful, that's all
# TODO : document that if kwargs are used, they need be explicitly written
# or else handle this on your own : i.e. foo(1, b=1) versus foo(1,1)

# Protocols here
class CircularAverage(Protocol):
    ''' Circular average.

        Note : Assumes square pixels
            Also assumes variance comes from shot noise only (by taking average along ring/Npixels)

        if bins is None, it does it's best to estimate pixel sizes and make the
        bins a pixel in size. Note, for Ewald curvature this is not
        straightforward. You need both a r_map in pixels from the center and
        the q_map for the actual q values.
    '''
    #TODO : extend file to mltiple writers?
    @run_default("XS:CircularAverage", xml_options={}, databroker=False,
                 file_options= {'writers' : [{'keys' : ['sqx', 'sqxerr', 'sqy', 'sqyerr'], 'writer' : 'npy'},
                                 {'keys' : ['sqx', 'sqxerr', 'sqy', 'sqyerr'], 'writer' : 'dat'}]},
                databroker_options={'name':'cms:analysis', 'writers'  : {'sqx':'npy', 'sqy' : 'npy', 'sqxerr' : 'npy', 'sqyerr' : 'npy'}})
    def run(image=None, q_map=None, r_map=None,  bins=None, mask=None, **kwargs):
        ''' computes the circular average.'''
        return _circavg(image=image, q_map=q_map, r_map=r_map, bins=bins, mask=mask, **kwargs)

class Thumbnail(Protocol):
    ''' Compute a thumbnail.
    '''
    @run_default("XS:Thumb", databroker=False, xml_options={}, file_options={'writers' : {'keys' : 'thumb', 'writer' : 'jpg'}},
            databroker_options={'name':'cms:analysis', 'writers'  : {'thumb': 'jpg'}})
    def run(image=None, mask=None, blur=None, crop=None, resize=None, type='linear', vmin=None, vmax=None):
        '''
            The process that creates the thumbnail.
            type :
                linear : don't do anything to image
                log : take log
        '''
        return _thumb(image=None, mask=None, blur=None, crop=None, resize=None, type='linear', vmin=None, vmax=None)


# Main functions

def _circavg(image=None, q_map=None, r_map=None, bins=None, mask=None, **kwargs):
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
                nobins = np.maximum(*(mask.shape))

            # here we assume the rbins uniform
            bins = nobins
            rbinstat = RadialBinnedStatistic(image.shape, bins=nobins,
                    rpix=r_map, statistic='mean', mask=mask)
            bin_centers = rbinstat(q_map)
            bins = _center2edge(bin_centers)


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

def _center2edge(centers, positive=True):
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
    if positive:
        diffedges = np.diff(edges)
        w, = np.where(diffedges > 0)
        edges = np.concatenate((edges[0].reshape(1), edges[w+1]))
    # cleanup nans....
    w = np.where(~np.isnan(edges))
    edges = edges[w]
    return edges


def _thumb(image=None, mask=None, blur=None, crop=None, resize=None, type='linear', vmin=None, vmax=None):
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
