# TODO : add pixel procesing/thresholding threshold_pixels((2**32-1)-1) # Eiger
# inter-module gaps

from .. import globals as streams_globals

from dask import set_options
set_options(delayed_pure=True)  # noqa

import numpy as np

# Detector information
from ..detectors.detectors2D import detectors2D
from ..detectors.detectors2D import _make_detector_name_from_key

from ..processing.stitching import xystitch_accumulate, xystitch_result
from ..processing.circavg import circavg
from ..processing.qphiavg import qphiavg
from ..processing.image import blur as _blur, crop as _crop, resize as _resize
from ..processing import rdpc
from ..processing.angularcorr import angular_corr
from ..processing.nn_fbbenet import infer
from ..processing.peak_finding import peak_finding


from ..config import config


'''
    Notes : load should be a separate function
'''

# from SciAnalysis.analyses.XSAnalysis.Data import Calibration
# use RQConv now
from ..data.Calibration import Calibration
import streamz as sc
import SciStreams.core.scistreams as scs
import SciStreams.core.StreamDoc as sd

keymaps = config['keymaps']

# NOTE : When defining streams, make sure to place the expected inputs
# and outputs in the docstrings! See stream below for a good example.

allowed_detector_keys = ['pilatus2M_image', 'pilatus300_image']


def pick_allowed_detectors(sdoc):
    ''' Only pass through 2d array events, ignore rest.

        It will output a list of sdocs, each with different detector.
    '''
    sdocs = list()
    md = sdoc.attributes
    kwargs = sdoc.kwargs
    for key in allowed_detector_keys:
        if key in kwargs:
            data = kwargs[key]
        else:
            continue
        if hasattr(data, 'ndim') and data.ndim > 0:
            # this picks data of dimensions 2 only
            # but also outputs some hints (in case we get a different
            # detector that outputs different data. for ex: time series etc)
            if data.ndim == 1:
                # TODO : return an sdoc that can raise something if needed
                print("Found a 1D line of data? Ignoring...")
                continue
            elif data.ndim == 3:
                msg = "Found 3D array data. Ignoring (but make sure this"
                msg += " is not something you want to analyze"
                continue
            elif data.ndim > 3:
                continue
        else:
            continue
        # everything is good, output this in list
        new_md = dict(md)
        new_md.update(detector_key=key)
        new_kwargs = dict(image=data)
        sdoc_new = sd.StreamDoc(kwargs=new_kwargs, attributes=new_md)
        sdocs.append(sdoc_new)

    return sdocs


# Streams : These return an sin and sout
def PrimaryFilteringStream():
    ''' Filter the stream for just primary results.
        Stream Inputs
        -------------

            md : No requirements

            data :
                must have a 2D np.ndarray with one of accepted detector
                    keys

        Stream Outputs
        --------------
            From 0 - any number streams
                (depends on how many detectors were found)

            md :
                detector_key : the detector key (string)
            data :
                data with only one image as detector key
                if there was more than one, it selects one of them
                Note this has unspecified behaviour.
    '''
    sin = sc.Stream(stream_name="Primary Filter")
    sout = sc.map(sin, pick_allowed_detectors)
    # turn list into individual streams
    # (if empty list, emits nothing, this is sort of like filter)
    sout = sout.concat()
    # just some checks to see if it's good data, else ignore
    return sin, sout


def AttributeNormalizingStream(external_keymap=None):
    sin = sc.Stream()
    sout = scs.get_attributes(sin)
    sout = scs.map(normalize_calib_dict, sout, external_keymap=external_keymap)
    sout = scs.map(add_detector_info, sout)
    return sin, sout


def normalize_calib_dict(external_keymap=None, **md):
    ''' Normalize the calibration parameters to a set of parameters that the
    analysis expects.

        Parameters
        ----------
        external_keymap : dict, optional
            external keymap to use to override
            (useful for testing mainly)

        It gives entries like:
            beamx0 : dict(value=a, unit=b)
        etc...
    '''
    if external_keymap is None:
        keymap_name = md.get("keymap_name", "cms")
        keymap = keymaps[keymap_name]
    else:
        keymap = external_keymap
    for key, val in keymap.items():
        name = val['name']
        if name is not None:
            # for debugging
            # print("setting {} to {}".format(name, key))
            # swap out temp vals
            tmpval = md.pop(name, val['default_value'])
            default_unit = val['default_unit']
            md[key] = dict(value=tmpval, unit=default_unit)

    return md


def add_detector_info(**md):
    '''
        Add detector information to the metadata, like shape etc.
        This is a useful step for 2D SAXS analysis, before making the
        calibration parameters.

    Expects:

        detector_name : the detector name
        img_shape : tuple, optional
            force the image shape. This is useful when the detector image
                has been transformed (i.e. image stitching)
    '''
    detector_key = md.get('detector_key', None)

    # only do something is there is a detector key
    if detector_key is not None:
        detector_name = _make_detector_name_from_key(detector_key)

        md['detector_name'] = detector_name

        # use the detector info supplied
        # look up in local library
        md['pixel_size_x'] = detectors2D[detector_name]['pixel_size_x']
        md['pixel_size_y'] = detectors2D[detector_name]['pixel_size_y']

        # shape is just a tuple, not a dict(value=...,unit=...)
        if 'shape' not in md:
            md['shape'] = detectors2D[detector_name]['shape']['value']
    else:
        msg = "Warning : no detector key found,"
        msg += " not adding detector information"
        print(msg)

    return md


def CalibrationStream():
    ''' This stream takes data with kwargs and creates calibration object.


       Stream Inputs
       -------------

             metadata : No requirements
             data : requires keys who contain calibration information
                (this is usually obtained by moving metadata to data in first
                step)
            Note : use the AttributeNormalizingStream first so that the data
            is as the CalibrationStream expects.

        Stream Outputs
        --------------

            md : keeps regular md
            data :
                calibration : a calibration object

        Notes
        -----
            This will distribute the computation of the qmaps and cache them
            by saving references to the futures (which distributed will
            bookkeep)

    '''
    sin = sc.Stream(stream_name="Calibration")
    sout = scs.map(make_calibration, sin)

    # this piece should be computed using Dask
    def _generate_qxyz_maps(calibration):
        calibration.generate_maps()
        return dict(calibration=calibration)

    # from streamz.dask import scatter, gather
    from SciStreams.globals import client

    # TODO : change to use scatter/gather
    # (need to setup event loop for this etc)

    sout = scs.map(lambda calibration:
                   client.submit(_generate_qxyz_maps, calibration),
                   sout)

    # save the futures to a list (scheduler will ensure caching of result if
    # any reference to a future is kept)
    sc.map(sout, lambda calibration:
           streams_globals.futures_cache.append(calibration))

    sout = scs.map(lambda calibration:
                   client.gather(calibration), sout)

    return sin, sout


def make_calibration(**md):
    '''
        Update calibration with all keyword arguments fill in the defaults

        This expects a dictionary of a certain form with certain elements:
            'wavelength'
            'pixel_size_x'
            'sample_det_distance'
            'beamx0'
            'beamy0'

        img_shape : specify arbitrary shape (useful for stitched images)
    '''
    # TODO : move detector stuff into previous load routine
    # k = 2pi/wv
    wavelength = md['wavelength']['value']
    md['k'] = dict(value=2.0*np.pi/wavelength, unit='1/Angstrom')
    # energy
    # h = 6.626068e-34  # m^2 kg / s
    c = 299792458  # m/s
    wavelength = md['wavelength']['value']  # in Angs *1e-10  # m
    # E = h*c/wavelength  # Joules
    # E *= 6.24150974e18  # electron volts
    # E /= 1000.0  # keV
    # calib_tmp.update(Singlet('energy', E, 'keV'))
    # q per pixel (Small angle limit)
    '''Gets the delta-q associated with a single pixel. This is computed in
    the small-angle limit, so it should only be considered a approximate.
    For instance, wide-angle detectors will have different delta-q across
    the detector face.'''
    c = (md['pixel_size_x']['value']/1e6) / \
        md['sample_det_distance']['value']
    twotheta = np.arctan(c)  # radians
    md['q_per_pixel'] = dict(value=2.0*md['k']['value']*np.sin(twotheta/2.0),
                             unit="1/Angstrom")

    # some post calculations

    pixel_size_um = md['pixel_size_x']['value']
    distance_m = md['sample_det_distance']['value']
    wavelength_A = wavelength

    # prepare the calibration object
    calib_object = Calibration(wavelength_A=wavelength_A,
                               distance_m=distance_m,
                               pixel_size_um=pixel_size_um)
    # NOTE : width, height reversed in calibration
    try:
        height, width = md['shape']
    except Exception:
        msg = "Error in the shape element of metadata"
        raise ValueError(msg)
    calib_object.set_image_size(width, height)
    calib_object.set_beam_position(md['beamx0']['value'],
                                   md['beamy0']['value'])
    # print("calibration object: {}".format(calib_object))
    # print("calibration object members: {}".format(calib_object.__dict__))

    return dict(calibration=calib_object)


def _generate_qxyz_maps(calibration):
    calibration.generate_maps()
    return dict(calibration=calibration)


def CircularAverageStream():
    ''' Circular average stream.

        Stream Inputs
        -------------
            (image, calibration, mask=, bins=)

            image : 2d np.ndarray
                the image to run circular average on

            calibration : 2D np.ndarray
                the calibration object, with members:

                q_map : 2d np.ndarray
                    the magnite of the wave vectors

                r_map : 2d np.ndarray
                    the pixel positions from center

            mask= : 2d np.ndarray, optional
                the mask

            bins= : int or tuple, optional
                if an int, the number of bins to divide into
                if a list, the bins to use

        Stream Outputs
        --------------
            sqx= : 1D np.ndarray
                the q values
            sqxerr= : 1D np.ndarray
                the error q values
            sqy= : 1D np.ndarray
                the intensities
            sqyerr= : 1D np.ndarray
                the error in intensities (approximate)

        Returns
        -------
            sin : Stream, the source stream (see Stream Inputs)
            sout : the output stream (see Stream Outputs)

        Notes
        -----
        - Assumes square pixels
        - Assumes variance comes from shot noise only (by taking average along
            ring/Npixels)
        - If bins is None, it does its best to estimate pixel sizes and make
            the bins a pixel in size. Note, for Ewald curvature this is not
            straightforward. You need both a r_map in pixels from the center
            and the q_map for the actual q values.

    TODO : Add options

    '''
    # TODO : extend file to mltiple writers?
    def validate(x):
        kwargs = x['kwargs']
        if 'image' not in kwargs or 'calibration' not in kwargs:
            message = "expected two kwargs: "
            message += "(image, calibration), "
            message += "got {} instead".format(kwargs.keys())
            raise ValueError(message)

        # kwargs are optional so don't validate them
        return x

    sin = sc.Stream(stream_name="Circular Average")
    sout = scs.add_attributes(sin, stream_name="circavg")
    sout = sout.map(validate)
    sout = scs.map(circavg_from_calibration, sout)

    return sin, sout


def circavg_from_calibration(image, calibration, mask=None, bins=None):
    # print("circavg : qmap : {} ".format(calibration.q_map))
    # print("circavg : rmap: {} ".format(calibration.r_map))
    return circavg(image, q_map=calibration.q_map, r_map=calibration.r_map,
                   mask=mask, bins=bins)


def QPHIMapStream(bins=(800, 360)):
    '''
        Parameters
        ----------
        bins : 2 tuple, optional
            the number of bins to divide into

        Stream Inputs
        -------------
            img : 2d np.ndarray
                the image
            mask : 2d np.ndarray, optional
                the mask
            origin : 2 tuple
                the beam center in the image
            qmap : 2d np.ndarray
                the qmap of the image

        Stream Outputs
        --------------
            sqphi= : 2d np.ndarray
                the sqphi map
            qs= : 1d np.ndarray
                the q values
            phis= : 1d np.ndarray
                the phi values

        Returns
        -------
        sin : the input stream (see Stream Inputs)
        sout : the output stream (see Stream Outputs)
    '''
    #

    sin = sc.Stream(stream_name="QPHI map Stream")
    sout = scs.map(qphiavg, sin, bins=bins)
    sout = scs.add_attributes(sout, stream_name="qphiavg")

    return sin, sout


def LineCutStream(axis=0, name=None):
    ''' Obtain line cuts from a 2D image.
        Just simple slicing. It's a stream mainly to make this more standard.

        Parameters
        ----------
            axis : int, optional
                the axis to obtain linecuts from.
                Default is 0 (so we index rows A[i])
                If 1, the index cols (A[:,i])

        Stream Inputs
        -------------
        image : 2d np.ndarray
            the image to obtain line cuts from
        y : 1d np.ndarray
            The y (row) values per pixel
        x : The x (column) values per pixel
        vals : list
            the values to obtain the linecuts from

        Stream Outputs
        --------------
        linecuts : a list of line cuts
        linecuts_domain : the domain of the line cuts
        linecuts_vals : the corresponding value for each line cut

        Returns
        -------
        sin : the input stream (see Stream Inputs)
        sout : the output stream (see Stream Outputs)
    '''
    def linecuts(image, y, x, vals, axis=0):
        ''' Can potentially return an empty list of linecuts.'''

        linecuts = list()
        linecuts_vals = list()
        if axis == 1:
            # swap x y and transpose
            tmp = y
            y = x
            x = tmp

        linecuts_domain = x
        for val in vals:
            ind = np.argmin(np.abs(y-val))
            linecuts.append(image[ind])
            linecuts_vals.append(y[ind])

        return dict(linecuts=linecuts, linecuts_domain=linecuts_domain,
                    linecuts_vals=linecuts_vals)

    # the string for the axis
    axisstr = ['y', 'x'][axis]
    if name is None:
        stream_name = 'linecuts-axis{}'.format(axisstr)
    else:
        stream_name = name + "-axis{}".format(axisstr)

    sin = sc.Stream(stream_name=stream_name)
    sout = scs.map(linecuts, sin, axis=axis)

    sout = scs.add_attributes(sout,
                              stream_name=stream_name)
    return sin, sout

def CollapseStream(axis=0, name="collapse-image"):
    ''' This stream collapses 2D images to 1D images
        by averaging along an axis.

        Stream Inputs
        -------------
        image : 2d np.ndarray
            the 2D image
        mask : 2d np.ndarray
            optional mask
    '''
    def collapse(image, mask=None, axis=0):
        if mask is None:
            # normalization is number of pixels in dimension
            # if no mask
            # TODO : Fix
            #norm = img.shape[
            norm = 1
        else:
            norm = np.sum(mask, axis=axis)
        cll = np.sum(image, axis=axis)
        res = cll/norm
        return dict(line=res, axis=axis)
    sin = sc.Stream(stream_name=name)

    sout = scs.map(collapse, axis=axis)
    return sin, sout


def AngularCorrelatorStream(bins=(800, 360)):
    ''' Stream to run angular correlations.

        Stream Inputs
        -------------
        image : 2d np.ndarray
            the image to run the angular correltions on
        mask : 2d np.ndarray
            the mask
        origin : 2 tuple
            the beam center of the image
        bins : tuple
            the number of bins in q and phi
        method : string, optional
            the method to use for the angular correlations
            defaults to 'bgest'
        q_map : the q_map to be used

        Stream Outputs
        --------------
        sin : the stream input
        sout : the stream output

        Incomplete
    '''
    # TODO : Allow optional kwargs in streams
    sin = sc.Stream(stream_name="Angular Correlation")
    sout = scs.select(sin, ('image', 'image'), ('mask', 'mask'),
                      ('origin', 'origin'), ('q_map', 'r_map'))
    sout = scs.map(angular_corr, sout, bins=bins)
    sout = scs.add_attributes(sout, stream_name="angular-corr")
    return sin, sout


def prepare_correlation(shape, origin, mask, rbins=800, phibins=360,
                        method='bgest'):
    rdphicorr = rdpc.RDeltaPhiCorrelator(shape,  origin=origin,
                                         mask=mask, rbins=rbins,
                                         phibins=phibins)
    # print("kwargs : {}".format(kwargs))
    return rdphicorr


def angularcorrelation(rdphicorr, image):
    ''' Run the angular correlation on the angular correlation object.'''
    rdphicorr.run(image)
    return rdphicorr.rdeltaphiavg_n


def _xystitch_result(img_acc, mask_acc, origin_acc, stitchback_acc):
    # print("_xystitch_result, img_acc : {}".format(img_acc))
    return xystitch_result(img_acc, mask_acc, origin_acc, stitchback_acc)


def _xystitch_accumulate(prevstate, newstate):
    # print("_xystitch_accumulate, prevstate: {}".format(prevstate))
    return xystitch_accumulate(prevstate, newstate)


# Image stitching Stream
def ImageStitchingStream(return_intermediate=False):
    '''
        Image stitching

        Stream Inputs
        -------------
            image= : 2d np.ndarray
                the image for the stitching
            mask= : 2d np.ndarray
                the mask
            origin= : 2 tuple
                the beam center
            stitchback= : bool
                whether or not to stitchback to previous image

        Stream Outputs
        --------------
            image= : 2d np.ndarray
                the stitched image
            mask= : 2d np.ndarray
                the mask from the stitch
            origin= : 2 tuple
                the beam center
            stitchback= : bool
                whether or not to stitchback to previous image

        Returns
        -------
            sin : the input stream
            sout : the output stream

        Parameters
        ----------
            return_intermediate : bool, optional
                decide whether to return intermediate results or not
                defaults to False

        Notes
        -----
        Any normalization of images (for ex: by exposure time) should be done
        before inputting to this stream.
    '''
    # TODO : add state. When False returned, need a reason why
    def validate(x):
        if not hasattr(x, 'kwargs'):
            raise ValueError("No kwargs")
        kwargs = x['kwargs']
        expected = ['mask', 'origin', 'stitchback', 'image']
        for key in expected:
            if key not in kwargs:
                message = "{} not in kwargs".format(key)
                raise ValueError(message)
        if not isinstance(kwargs['mask'], np.ndarray):
            message = "mask is not array"
            raise ValueError(message)

        if not isinstance(kwargs['image'], np.ndarray):
            message = "image is not array"
            raise ValueError(message)

        if len(kwargs['origin']) != 2:
            message = "origin not length 2"
            raise ValueError(message)
        return x

    # TODO : remove the add_attributes part and just keep stream_name
    sin = sc.Stream(stream_name="Image Stitching Stream")
    sout = sc.map(sin, validate)
    # sin.map(lambda x : print("Beginning of stream data\n\n\n"))
    # TODO : remove compute requirement
    # TODO : incomplete
    sout = scs.add_attributes(sout, stream_name="stitch")

    sout = scs.select(sout, ('image', None), ('mask', None), ('origin', None),
                      ('stitchback', None))

    # put all args into a tuple
    def pack(*args):
        return args

    sout = scs.map(pack, sout)
    # sout = scs.map(s3.map(psdm(pack))
    sout = scs.accumulate(_xystitch_accumulate, sout)

    sout = scs.map(scs.star(_xystitch_result), sout)

    def stitchbackcomplete(xtuple):
        ''' only plot images whose stitch is complete, and only involved more
        than one image
        NOTE : *Only* the bool "True" will activate a stitch. "1" does not
        count. This is handled by checking 'is True' and 'is not True'
        '''
        # previous must have been true for stitch to have involved more than
        # one image
        prev = xtuple[0]['kwargs']['stitchback']
        # next must be False (or just not True) to be complete
        next = xtuple[1]['kwargs']['stitchback']

        return next is not True and prev is True

    # swin.map(lambda x : print("result : {}".format(x)), raw=True)

    # only get results where stitch is stopped
    # NOTE : need to compute before filtering here

    # now emit some dummy value to swin, before connecting more to stream
    # swin.emit(dict(attributes=dict(stitchback=0)))

    if not return_intermediate:
        # keep previous two results
        sout = sout.sliding_window(2)
        sout = sout.filter(stitchbackcomplete)
        sout = sout.map(lambda x: x[0])

    return sin, sout


def ThumbStream(blur=None, crop=None, resize=None):
    ''' Thumbnail stream

        Parameters
        ----------
            blur : float, optional
                the sigma of the Gaussian kernel to convolve image with
                    for smoothing
                default is None, no smoothing

            crop : 4 tuple of int, optional
                the boundaries to crop by
                default is None, no cropping

            resize : int, optional
                the factor to resize by
                for example resize=2 performs 2x2 binning of the image

        Stream Inputs
        -------------
            image : 2d np.ndarray
                the image

        Returns
        -------
            sin : the stream input
            sout : the stream output

    '''
    # TODO add flags to actually process into thumbs
    sin = sc.Stream(stream_name="Thumbnail Stream")
    sout = scs.add_attributes(sin, stream_name="thumb")
    # s1 = sin.add_attributes(stream_name="ThumbStream")
    sout = scs.map(_blur, sout, sigma=blur)
    sout = scs.map(_crop, sout, crop=crop)
    sout = scs.map(_resize, sout, resize=resize)
    # change the key from image to thumb
    sout = scs.select(sout, ('image', 'thumb'))

    return sin, sout


#
def PeakFindingStream():
    '''
        Stream Inputs
        -------------

        Stream Outputs
        -------------
    '''
    sin = sc.Stream(stream_name="Peak Finder")
    # pkfind stream
    sout = scs.map(call_peak, scs.select(sin, 'sqy', 'sqx'))
    sout = scs.add_attributes(sout, stream_name='peakfind')
    return sin, sout


# try peak finding code
def call_peak(sqx, sqy):
    res = peak_finding(intensity=sqy, frac=0.0001).peak_position()

    model = res[0]
    y_origin = res[1]
    inds_peak = res[2]
    xdata = res[3]
    ratio = res[4]
    ydata = res[5]
    wdata = res[6]
    bkgd = res[7]
    variance = res[8]
    variance_mean = res[9]

    peaksx = list()
    peaksy = list()

    for ind in inds_peak:
        peaksx.append(sqx[ind])
        peaksy.append(sqy[ind])

    res_dict = dict(
            model=model,
            y_origin=y_origin,
            inds_peak=inds_peak,
            xdata=xdata,
            ratio=ratio,
            ydata=ydata,
            wdata=wdata,
            bkgd=bkgd,
            variance=variance,
            variance_mean=variance_mean,
            peaksx=peaksx,
            peaksy=peaksy,
            )

    return res_dict


# these are all the nn steps
def ImageTaggingStream():
    ''' Creates an image taggint stream.

        Stream Inputs
        -------------
        image : the image to be tagged

        Stream Outputs
        --------------
        tag_name : the name of the tag for the image
    '''
    sin = sc.Stream(stream_name="Image Tagger")
    sout = scs.map(infer, scs.select(sin, 'image'))
    sout = scs.add_attributes(sout, stream_name="image-tag")
    return sin, sout


def PCAStream(Nimgs=100, n_components=16):
    '''
        This runs principle component analysis on the last Nimgs

        Stream Inputs
        -------------
        'image' : the nth image

        Stream Outputs
        --------------
        components : the components

        Returns
        -------
        sin : the input stream
        sout : the output stream

        you need to connect these streams for them to be useful
    '''
    sin = sc.Stream(stream_name="PCA Stream")
    sout = sin.sliding_window(Nimgs)
    sout = scs.select(sin, ('image', 'data'))
    sout = scs.squash(sin)
    sout = scs.map(sout, PCA_fit, n_components=n_components)

    return sin, sout


# sample custom written function
def PCA_fit(data, n_components=10):
    ''' Run principle component analysis on data.
        n_components : num components (default 10)
    '''
    # first reshape data if needed
    if data.ndim > 2:
        datashape = data.shape[1:]
        data = data.reshape((data.shape[0], -1))

    from sklearn.decomposition import PCA
    pca = PCA(n_components=n_components)
    pca.fit(data)
    components = pca.components_.copy()
    components = components.reshape((n_components, *datashape))
    return dict(components=components)
