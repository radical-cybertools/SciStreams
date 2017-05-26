# run stream on a relocatable machine i.e. not datbroker dependent
# test a XS run
import os
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
# this is interactive mode for debugging
plt.ion()

# dask imports
# set up the distributed client
#from dask.cache import Cache
#_cache = Cache(1e9)
#_cache.register()

#from distributed import Client
#client = Client("10.11.128.3:8786")

from collections import deque

from globals import client

from dask import set_options, delayed, compute
# assume all functions are pure globally
set_options(delayed_pure=True)

# misc imports
import sys
import SciAnalysis.config as config
import numpy as np

# SciAnalysis imports
## interfaces
#from SciAnalysis.interfaces.databroker import databroker as source_databroker
from SciAnalysis.interfaces.plotting_mpl import plotting_mpl as source_plotting
from SciAnalysis.interfaces.file import file as source_file
from SciAnalysis.interfaces.detectors import detectors2D
## Analyses
#from SciAnalysis.analyses.XSAnalysis.Protocols_streams import CircularAverage, Thumbnail
## Data
# the stream version

#from SciAnalysis.interfaces.databroker.databases import databases
#cddb = databases['cms:data']


# Streams include stuff
# select needed for mapping
from SciAnalysis.interfaces.StreamDoc import StreamDoc, Stream,\
    parse_streamdoc, Arguments
#from streams.core import Stream

# This decorator is necessary to provide a way to find out what stream doc is
# unique. We need to tokenize only the args, kwargs and attributes of
# StreamDoc, nothing else
from SciAnalysis.interfaces.StreamDoc import delayed_wrapper
#def inspect_stream(stream):
    #stream.apply(compute).apply(lambda x : print(x[0]))

def get_attributes(sdoc):
    # reasoning behind this: need to compute the attributes before they're
    # evaluated in function, else it gets passed as delayed reference
    return StreamDoc(args=sdoc['attributes'])

def add_attributes(sdoc, **attr):
    # make a copy
    newsdoc = StreamDoc(sdoc)
    newsdoc.add(attributes=attr)
    return newsdoc

def get_stitchback(attr, *args, **kwargs):
    #print(attr['stitch'])
    #print("kwargs : {}".format(kwargs))
    #print("args : {}".format(args))
    return attr['stitchback'],

# TODO merge check with get stitch
def check_stitchback(sdoc):
    if 'stitchback' not in sdoc['attributes']:
        sdoc['attributes']['stitchback'] = 0
    return StreamDoc(sdoc)

def get_exposure_time(attr, *args, **kwargs):
    return attr['sample_exposure_time']

def norm_exposure(image=None, exposure_time=None, **kwargs):
    return image/exposure_time

def multiply(A, B):
    return A*B

def divide(A, B):
    return A/B

def set_detector_name(sdoc, detector_name='pilatus300'):
    sdoc['attributes']['detector_name'] = detector_name
    return StreamDoc(sdoc)

def safelog10(img):
    img_out = np.zeros_like(img)
    w = np.where(img > 0)
    img_out[w] = np.log10(img[w])
    return img_out

def PCA_fit(image=None, n_components=10):
    ''' Run principle component analysis on data.
        n_components : num components (default 10)
    '''
    data = image
    # first reshape data if needed
    if data.ndim > 2:
        datashape = data.shape[1:]
        data = data.reshape((data.shape[0], -1))

    from sklearn.decomposition import PCA
    pca = PCA(n_components=n_components)
    pca.fit(data)
    components = pca.components_.copy()
    components = components.reshape((n_components, *datashape))
    return Arguments(components=components)

# TODO :  need to fix this
@delayed
def squash(sdocs):
    newsdoc = StreamDoc()
    for sdoc in sdocs:
        newsdoc.add(attributes = sdoc['attributes'])
    N = len(sdocs)
    cnt = 0
    newargs = []
    newkwargs = dict()
    for sdoc in sdocs:
        args, kwargs = sdoc['args'], sdoc['kwargs']
        for i, arg in enumerate(args):
            if cnt == 0:
                if isinstance(arg, np.ndarray):
                    newshape = []
                    newshape.append(N)
                    newshape.extend(arg.shape)
                    newargs.append(np.zeros(newshape))
                else:
                    newargs.append([])
            if isinstance(arg, np.ndarray):
                newargs[i][cnt] = arg
            else:
                newargs[i].append[arg]

        for key, val in kwargs.items():
            if cnt == 0:
                if isinstance(val, np.ndarray):
                    newshape = []
                    newshape.append(N)
                    newshape.extend(val.shape)
                    newkwargs[key] = np.zeros(newshape)
                else:
                    newkwargs[key] = []
            if isinstance(val, np.ndarray):
                newkwargs[key][cnt] = val
            else:
                newkwargs[key].append[val]

        cnt = cnt + 1

    newsdoc.add(args=newargs, kwargs=newkwargs)

    return newsdoc

def isSAXS(sdoc):
    ''' return true only if a SAXS expt.'''
    attr = sdoc['attributes']
    if 'experiment_type' in attr:
        #print("experiment type : {}".format(attr['experiment_type']))
        expttype = attr['experiment_type']
        if expttype == 'SAXS' or expttype == 'TSAXS':
            return True
    return False

def plot_recent(components=None):
    if components is None:
        pass

    ncomponents = len(components)
    n = int(np.ceil(np.sqrt(ncomponents)))

    plt.figure(0);plt.clf()
    for i in range(ncomponents):
        plt.subplot(n, n, i+1)
        plt.imshow(components[i])

    plt.savefig("recent.png")


globaldict = dict()

import dask
def compute(obj):
    ''' slight modification to remove this tuple being returned.'''
    return dask.compute(obj)[0]

from functools import partial
# Stream setup, datbroker data comes here (a header for now)
sin = Stream(wrapper=delayed_wrapper)
sout = sin.partition(10)
sout = sout.apply(squash)
sout = sout.map(PCA_fit)
sout.map(plot_recent).apply(compute)


import os.path
DDIR = os.path.expanduser("~/research/projects/pyCXD/storage")
dataset = np.load(DDIR + "/0670-matrix-trainingsets.npz")

Xtrain = dataset['Xtrain']
ytrain = dataset['ytrain']

vals = np.argmax(ytrain, axis=1)
names = np.array(['type 1', 'type 2', 'type 3', 'type 4'])
typenames = names[vals]


cnt = 0
for image, type in zip(Xtrain, typenames):
    sdoc = StreamDoc(kwargs=dict(image=image), attributes=dict(type=typenames))
    #print(image)
    print("sending image {}".format(cnt))
    cnt +=1 
    sin.emit(sdoc)
