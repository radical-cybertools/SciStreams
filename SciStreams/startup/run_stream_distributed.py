# this is the distributed version. It assumes that the StreamDocs are handled
# as Futures
# test a XS run
import numpy as np
import matplotlib
matplotlib.use("Agg")  # noqa

import matplotlib.pyplot as plt
plt.ion()  # noqa

# databroker
from databroker.assets.handlers import AreaDetectorTiffHandler

# if using dask async stuff will need this again
# from tornado.ioloop import IOLoop
# from tornado import gen

# from distributed import sync

from SciStreams.callbacks import CallbackBase, SciStreamCallback

from SciStreams.detectors.mask_generators import generate_mask

# from SciStreams.core.StreamDoc import StreamDoc
# StreamDoc to event stream
# from SciStreams.core.StreamDoc import to_event_stream
# import SciStreams.core.StreamDoc as sd

# the differen streams libs
import streamz.core as sc
import SciStreams.core.scistreams as scs


# import the different functions that make streams
from SciStreams.streams.XS_Streams import PrimaryFilteringStream
from SciStreams.streams.XS_Streams import AttributeNormalizingStream
from SciStreams.streams.XS_Streams import CalibrationStream
from SciStreams.streams.XS_Streams import CircularAverageStream
from SciStreams.streams.XS_Streams import PeakFindingStream
from SciStreams.streams.XS_Streams import QPHIMapStream
from SciStreams.streams.XS_Streams import ImageStitchingStream
from SciStreams.streams.XS_Streams import LineCutStream
from SciStreams.streams.XS_Streams import ThumbStream
from SciStreams.streams.XS_Streams import AngularCorrelatorStream
from SciStreams.streams.XS_Streams import ImageTaggingStream

# useful image normalization tool for plotting
from SciStreams.tools.image import normalizer

# interfaces imports
from SciStreams.interfaces.xml.xml import store_results_xml
from SciStreams.interfaces.hdf5 import store_results_hdf5


# TODO : move these out and put in stream folder
# use reg stream mapping
# from SciStreams.streams.XS_Streams import normalize_calib_dict,\
#   add_detector_info
from SciStreams.streams.XS_Streams import make_calibration


class LivePlot_Custom(CallbackBase):
    def start(self, doc):
        self.fignum = plt.figure().number

    def event(self, doc):
        img = doc[0]['data']['image']

        attrs = doc[1]['data']
        xkey = 'beamx0'
        ykey = 'beamy0'
        if xkey in attrs:
            x0 = attrs[xkey]['value']
            y0 = attrs[ykey]['value']
        else:
            x0, y0 = None, None

        plt.figure(self.fignum)
        plt.clf()
        plt.imshow(img)
        plt.clim(0, 100)
        if x0 is not None and y0 is not None:
            plt.plot(x0, y0, 'ro')


# We need to normalize metadata, which is used for saving, somewhere
# in our case, it's simpler to do this in the beginning
sin = sc.Stream(stream_name="Input")
stream_input = SciStreamCallback(sin.emit)

# these are abbreviations just to make streams access easier
# this stream filters out data. only outputs data that will work in rest of
# stream
sin_primary, sout_primary = PrimaryFilteringStream()
sin.connect(sin_primary)

# sink to list for debugging
L_primary = sout_primary.sink_to_list()


# get the attributes, clean them up and return
# new sout_primary
sin_attributes, sout_attributes = AttributeNormalizingStream()
L_attributes = sout_attributes.sink_to_list()
sout_primary.connect(sin_attributes)

sout_primary = scs.merge(sc.zip(sout_primary,
                         scs.to_attributes(sout_attributes)))

sin_calib, sout_calib = CalibrationStream()
sout_attributes.connect(sin_calib)
L_calib = sout_calib.sink_to_list()

#sout_calib2 = sout_calib
#sout_calib = sc.Stream()

# the PrimaryFilteringStream already split the detectors
#s_image = sc.Stream()
s_image = scs.add_attributes(sout_primary, stream_name="image")
L_image = s_image.sink_to_list()


# TODO : fix and remove this is for pilatus300 should be in mask gen
s_mask = scs.map(generate_mask, sout_attributes)
L_mask = s_mask.sink_to_list()


##s_zipped = 
##L_zipped= s_zipped.sink_to_list()
s_imgmaskcalib = scs.merge(sc.zip(s_image, sout_calib, s_mask))
L_imgmaskcalib = s_imgmaskcalib.sink_to_list()


# some small streams
def get_origin(**kwargs):
    ''' get the origin from the attributes.'''
    x = kwargs.get('beamx0', None)
    y = kwargs.get('beamy0', None)
    if x is None or y is None:
        origin = None
    else:
        origin = (y['value'], x['value'])

    return dict(origin=origin)


def get_exposure(**kwargs):
    return dict(exposure_time=kwargs.get('sample_exposure_time', None))


def get_stitch(**kwargs):
    return dict(stitchback=kwargs.get('stitchback', False))


s_origin = scs.map(get_origin, sout_attributes)

s_exposure = scs.map(get_exposure, sout_attributes)

s_stitch = scs.map(get_stitch, sout_attributes)
# name the stream for proper output

# circular average
sin_circavg, sout_circavg = CircularAverageStream()
s_imgmaskcalib.connect(sin_circavg)
L_circavg = sout_circavg.sink_to_list()

# peak finding
sin_peakfind, sout_peakfind = PeakFindingStream()
sout_circavg.connect(sin_peakfind)

# merge with sq
sout_sqpeaks = scs.merge(sc.zip(sout_circavg, scs.select(sout_peakfind,
                                'inds_peak', 'peaksx', 'peaksy')))

L_sqpeaks = sout_sqpeaks.sink_to_list()


def normexposure(image, exposure_time):
    return dict(image=image/exposure_time)


# image stitching
# normalize by exposure time
s_imagenorm = scs.map(normexposure, scs.merge(sc.zip(s_exposure, s_image)))
# use this for image stitch
s_imgmaskoriginstitch = scs.merge(sc.zip(s_imagenorm,
                                         s_mask,
                                         s_origin,
                                         s_stitch))


sin_stitched, sout_stitched = ImageStitchingStream(return_intermediate=True)
# NOTE : disconnected image stitching
s_imgmaskoriginstitch.connect(sin_stitched)

L_stitched = sout_stitched.sink_to_list()


def get_shape(**kwargs):
    img = kwargs.get('image', None)
    origin = kwargs.get('origin', None)

    # this is to make a new calibration object for stitched images
    if img is None:
        raise ValueError("get_shape : img is None")

    if origin is None:
        raise ValueError("get_shape : origin is None")

    y0, x0 = origin

    return dict(origin=origin, shape=img.shape)


# TODO : only spawn new process if a condition is met
sout_stitched_attributes = scs.map(get_shape, sout_stitched)
sout_stitched_attributes = scs.merge(sc.zip(sout_attributes,
                                            sout_stitched_attributes))
s_calib_stitched = scs.map(make_calibration, sout_stitched_attributes)


# the masked image. sometimes useful to use
def maskimg(image, mask):
    return dict(image=image*mask)


s_maskedimg = scs.map(maskimg, scs.select(s_imgmaskcalib, 'image', 'mask'))
L_maskedimg = s_maskedimg.sink_to_list()

# make qphiavg image
#
s_img_mask_origin = scs.merge(sc.zip(s_image, s_mask, s_origin))
s_qmap = scs.map(lambda calibration: dict(q_map=calibration.q_map),
                 sout_calib)
s_img_mask_origin_qmap = scs.merge(sc.zip(s_img_mask_origin, s_qmap))

sin_qphiavg, sout_qphiavg = QPHIMapStream()
s_img_mask_origin_qmap.connect(sin_qphiavg)

L_qphiavg = sout_qphiavg.sink_to_list()

sout_sqphipeaks = scs.merge(sc.zip(sout_qphiavg, scs.select(sout_peakfind,
                                                            'inds_peak',
                                                            'peaksx',
                                                            'peaksy')))
sout_sqphipeaks = scs.select(sout_sqphipeaks, ('sqphi', 'image'), ('qs', 'y'),
                             ('phis', 'x'), ('peaksx', 'vals'))

L_sqphipeaks = sout_sqphipeaks.sink_to_list()

sin_linecuts, sout_linecuts = LineCutStream(axis=0)
sout_sqphipeaks.connect(sin_linecuts)
L_linecuts = sout_linecuts.sink_to_list()


# TODO : connect the image to this
sin_gisaxs_linecutsx, sout_gisaxs_linecutsx = \
    LineCutStream(axis=0, name="gisaxs-linecuts-x")

sin_gisaxs_linecutsy, sout_gisaxs_linecutsy = \
    LineCutStream(axis=1, name="gisaxs-linecuts-y")

sin_thumb, sout_thumb = ThumbStream(blur=2, crop=None, resize=10)
s_image.connect(sin_thumb)

sin_angularcorr, sout_angularcorr = AngularCorrelatorStream(bins=(800, 360))
s_img_mask_origin_qmap.connect(sin_angularcorr)


L_angularcorr = sout_angularcorr.sink_to_list()

sout_angularcorrpeaks = scs.merge(sc.zip(sout_angularcorr,
                                         scs.select(sout_peakfind,
                                                    'inds_peak',
                                                    'peaksx',
                                                    'peaksy')))
sout_angularcorrpeaks = scs.select(sout_angularcorrpeaks,
                                   ('rdeltaphiavg_n', 'image'),
                                   ('qvals', 'y'),
                                   ('phivals', 'x'),
                                   ('peaksx', 'vals'))

sin_linecuts_angularcorr, sout_linecuts_angularcorr = \
    LineCutStream(axis=0, name="angularcorr")
sout_angularcorrpeaks.connect(sin_linecuts_angularcorr)
L_linecuts_angularcorr = sout_linecuts_angularcorr.sink_to_list()


sin_tag, sout_tag = ImageTaggingStream()
s_maskedimg.connect(sin_tag)
# s_maskedimg.sink(lambda x : print("masked img : {}".format(x)))
L_tag = sout_tag.sink_to_list()


# sample on how to plot to callback and file
# (must make it an event stream again first)
# set to True to enable plotting (opens many windows)
liveplots = False
# NOTE : disabled sinking
if False:
    # make event streams for some sinks
    event_stream_img = scs.to_event_stream(s_image)
    event_stream_sqphi = scs.to_event_stream(sout_qphiavg)
    event_stream_sq = scs.to_event_stream(sout_circavg)
    event_stream_peaks = scs.to_event_stream(sout_sqpeaks)
    event_stream_maskedimg = scs.to_event_stream(s_maskedimg)
    event_stream_stitched = scs.to_event_stream(sout_stitched)
    event_stream_linecuts = scs.to_event_stream(sout_linecuts)
    event_stream_thumb = scs.to_event_stream(sout_thumb)
    event_stream_angularcorr = scs.to_event_stream(sout_angularcorr)
    event_stream_linecuts_angularcorr = \
        scs.to_event_stream(sout_linecuts_angularcorr)
    event_stream_tag = scs.to_event_stream(sout_tag)

    if liveplots:
        from SciStreams.callbacks.live import LiveImage, LivePlot
        liveplot_sq = LivePlot('sqy', x='sqx', logx=True, logy=True)
        liveimage_img = LiveImage('image', cmap="inferno", tofile="image.png",
                                  norm=normalizer)
        liveimage_sqphi = LiveImage('sqphi', cmap="inferno", aspect="auto",
                                    tofile="sqphi.png", norm=normalizer)
        liveimage_maskedimg = LiveImage('image', cmap="inferno", aspect="auto",
                                        tofile="masked_image.png",
                                        norm=normalizer)

        # sample on how to make an event stream again
        # turn outputs into event streams first (for databroker
        # compatibility in the future)

        # output to plotting  callbacks
        event_stream_img.sink(scs.star(liveimage_img))
        event_stream_sqphi.sink(scs.star(liveimage_sqphi))
        event_stream_sq.sink(scs.star(liveplot_sq))
        event_stream_maskedimg.sink(scs.star(liveimage_maskedimg))

    # output to storing callbacks
    from SciStreams.callbacks.saving_mpl.core import StorePlot_MPL
    plot_storage_img = StorePlot_MPL(images=['image'], img_norm=normalizer)
    plot_storage_stitch = StorePlot_MPL(images=['image'], img_norm=normalizer)
    plot_storage_sq = StorePlot_MPL(lines=[('sqx', 'sqy')])
    plot_storage_sqphi = StorePlot_MPL(images=['sqphi'], img_norm=normalizer)
    plot_storage_peaks = StorePlot_MPL(lines=[dict(x='sqx', y='sqy'),
                                       dict(x='peaksx', y='peaksy', marker='o',
                                       color='r', linewidth=0)])
    plot_storage_linecuts = StorePlot_MPL(linecuts=[('linecuts_domain',  # x
                                                     'linecuts',  # y
                                                     'linecuts_vals')])  # val
    plot_storage_thumb = StorePlot_MPL(images=['thumb'], img_norm=normalizer)
    plot_storage_angularcorr = StorePlot_MPL(images=['rdeltaphiavg_n'],
                                             img_norm=normalizer,
                                             plot_kws=dict(vmin=0, vmax=1))

    plot_storage_linecuts_angularcorr = \
        StorePlot_MPL(linecuts=[('linecuts_domain',  # x
                                 'linecuts',  # y
                                 'linecuts_vals')])  # value

    scs.sink(scs.star(plot_storage_img), event_stream_img)
    scs.sink(scs.star(plot_storage_stitch), event_stream_stitched)
    scs.sink(scs.star(plot_storage_sq), event_stream_sq)
    scs.sink(scs.star(plot_storage_sqphi), event_stream_sqphi)
    scs.sink(scs.star(plot_storage_peaks), event_stream_peaks)
    sc.sink(event_stream_peaks,
            scs.star(SciStreamCallback(store_results_hdf5)))
    scs.sink(scs.star(plot_storage_linecuts), event_stream_linecuts)
    scs.sink(scs.star(plot_storage_thumb), event_stream_thumb)
    scs.sink(scs.star(plot_storage_angularcorr), event_stream_angularcorr)
    scs.sink(scs.star(plot_storage_linecuts_angularcorr),
             event_stream_linecuts_angularcorr)

    from SciStreams.callbacks.core import SciStreamCallback
    # save the peaks info
    sc.sink(event_stream_peaks,
            scs.star(SciStreamCallback(store_results_hdf5)))

    sc.sink(event_stream_img,
            scs.star(SciStreamCallback(store_results_hdf5)))

    sc.sink(event_stream_tag,
            scs.star(SciStreamCallback(store_results_xml)))
    sc.sink(event_stream_tag,
            scs.star(SciStreamCallback(store_results_hdf5)))
    # scs.map(print, event_stream_img)


# getting and sending data
class TiffHandler(AreaDetectorTiffHandler):
    def __call__(self, point_number):
        # if File not Found, return None
        try:
            res = AreaDetectorTiffHandler.__call__(self, point_number)
        except FileNotFoundError:
            print("File not found {}".format(next(self._fnames_for_point(0))))
            res = None
        return res


if True:
    # patchy way to get stream for now, need to fix later
    from SciStreams.interfaces.databroker.databases import databases

    cmsdb = databases['cms:data']
    # register a handler that ignores file not found
    # cmsdb.reg.register_handler("AD_TIFF", TiffHandler, overwrite=True)

    # hdrs = cmsdb(start_time="2017-07-13", stop_time="2017-07-14")# 16:00")
    # hdrs = cmsdb(start_time="2017-09-13", stop_time="2017-09-14 16:00")
    # for this data, beam center is 718, 598 (x,y)
    # so origin : 598, 718 (y, x)
    # need to add in motor positions
    # hdrs = cmsdb(start_time="2017-09-08", stop_time="2017-09-09")
    # Oleg data 2017
    # hdrs = cmsdb(start_time="2017-07-15", stop_time="2017-07-17")
    # some recent saxs data
    hdrs = cmsdb(start_time="2017-10-10", stop_time="2017-10-11")
    stream = cmsdb.restream(hdrs, fill=True)

elif False:
    print("Creating simulated stream")
    from SciStreams.tests.simulators import generate_event_stream
    x0, y0 = 743, 581.
    detx, dety = -65, -72
    peaks = [40, 80, 100, 120, 200, 300, 400, 700, 1000, 1300, 1500, 2000,
             2500, 2600]
    peakamps = [.0003]*len(peaks)
    sigma = 6.

    md = dict(sample_name="test",
              motor_bsx=-15.17,
              motor_bsy=-16.9,
              motor_bsphi=-12,
              # these get filled in in loop
              # motor_SAXSx = -65,
              # motor_SAXSy = -72.,
              # detector_SAXS_x0_pix=x0,
              # detector_SAXS_y0_pix=y0,
              # scan_id=0,
              detector_SAXS_distance_m=5.,
              calibration_energy_keV=13.5,
              calibration_wavelength_A=0.9184,
              experiment_alias_directory="/GPFS/xf11bm/data/2017_3/Simulated",
              experiment_cycle="2017_3",
              experiment_group="SciStream-test",
              filename="foo.tiff",
              # updated every time
              # sample_savename="out",
              sample_exposure_time=10.,
              stitchback=True)

    from SciStreams.simulators.saxs import mkSAXS
    from SciStreams.simulators.gisaxs import mkGISAXS

    from SciStreams.detectors.detectors2D import detectors2D
    shape = detectors2D['pilatus2M']['shape']['value']
    scl = detectors2D['pilatus2M']['pixel_size_x']['value']

    stream = list()
    shiftsx = [-6, 0, 6]
    shiftsy = [-8, 0, 8]
    scan_id = 0
    sym = 6  # 2*np.int(np.random.random()*12)
    phase = 2*np.pi*np.random.random()
    for shiftx in shiftsx:
        for shifty in shiftsy:
            x1 = x0 - shiftx*scl
            y1 = y0 - shifty*scl
            detx1, dety1 = detx+shiftx, dety+shifty

            md = md.copy()
            md.update(detector_SAXS_x0_pix=x1)
            md.update(detector_SAXS_y0_pix=y1)
            md.update(motor_SAXSx=detx1)
            md.update(motor_SAXSy=dety1)
            md.update(scan_id=scan_id)
            md.update(measurement_type="SAXS")
            md.update(sample_savename="sample_x{}_y{}".format(detx1, dety1))

            data = mkSAXS(shape, peaks, peakamps, phase, x1, y1, sigma, sym)

            plt.figure(1)
            plt.clf()
            plt.imshow(data)
            plt.pause(.1)

            data_dict = dict(pilatus2M_image=data)
            stream.extend(generate_event_stream(data_dict,
                                                md=md))
            scan_id += 1

    # try some GISAXS patterns
    shiftx, shifty = 0, 0
    x1 = x0 - shiftx*scl
    y1 = y0 - shifty*scl
    detx1, dety1 = detx+shiftx, dety+shifty
    r = 3
    ld = 12
    Narray = 5
    md = md.copy()
    md.update(measurement_type="GISAXS")
    md.update(stitchback=False)
    data = mkGISAXS(shape, r, ld, Narray, x1, y1)
    data_dict = dict(pilatus2M_image=data)
    stream.extend(generate_event_stream(data_dict,
                                        md=md))


# some loop over stream
# TODO look for FileNotFoundError in nds iteration
while True:
    try:
        print("iterating")
        nds = next(stream)
        stream_input(*nds)
        plt.pause(.1)
    except StopIteration:
        break
    except FileNotFoundError:
        continue
