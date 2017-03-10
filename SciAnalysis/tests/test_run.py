import sys, os
# no need as SciAnalysis is globally installed in conda env
#SciAnalysis_PATH = os.path.expanduser("~/home/xf11bm/software/SciAnalysis")
#SciAnalysis_PATH in sys.path or sys.path.append(SciAnalysis_PATH)

import glob
from SciAnalysis import tools
from SciAnalysis.XSAnalysis.Data import Calibration, Mask
from SciAnalysis.XSAnalysis import Protocols
from SciAnalysis.Data import cmap_vge_hdr

import tempfile
from PIL import Image

# set up mock database
from portable_mds.sqlite.mds import MDS as MDS_Analysis
from portable_fs.sqlite.fs import FileStore as FileStore_Analysis
from portable_fs.sqlite.fs import FileStore as FileStore_Analysis
from filestore.path_only_handlers import RawHandler
from filestore import HandlerBase
from filestore.handlers import DATHandler
from databroker import Broker
import numpy as np
from SciAnalysis.detectors import detectors2D


def test_thumb(db=None, header=None):
    ''' Test the thumbnail code for analysis.
        This is a good example. The database needs to be passed to the
            Processor object.

        If db is not none, read image from there
        If header is not None, use that as database to retrieve an image from
            should be a tuple of database, uid, detector_key
    '''
    #from setupdb import cmsdb_analysis

    # Set up of the databroker
    # This an example. You'll need to know your local configuration.
    tmpfile = tempfile.NamedTemporaryFile().name
    tmpdir_analysis = tempfile.TemporaryDirectory().name
    os.mkdir(tmpdir_analysis)
    tmpdir_data = tempfile.TemporaryDirectory().name
    os.mkdir(tmpdir_data)

    mds_analysis_conf = {
                         'database': 'metadatastore-production-v1',
                         'timezone': 'US/Eastern',
                         # test directory
                         'directory' : tmpdir_analysis
                         }

    mds_analysis = MDS_Analysis(mds_analysis_conf, auth=False)
    # This an example. You'll need to know your local configuration.
    fs_analysis_conf = {
                        'database': 'filestore-production-v1',
                        # test path
                         'dbpath' : tmpfile
                        }
    # if first time, run this:
    #from filestore.utils import install_sentinels
    #install_sentinels(fs_analysis_conf, version_number)
    fs_analysis = FileStore_Analysis(fs_analysis_conf)

    cmsdb_analysis = Broker(mds_analysis, fs_analysis)
    # reverse key map in databroker metadata
    keymaps = {'wavelength_A' : 'calibration_wavelength_A',
                    'detectors' : 'detectors',
                    'beamx0' : 'detector_SAXS_x0_pix',
                    'beamy0' : 'detector_SAXS_y0_pix',
                    'sample_det_distance_m' : 'detector_SAXS_distance_m'}

    


    if header is not None:
        if db is None:
            raise ValueError("Error: header and db must be set")

        start_doc = header['start']
        wavelength_A = start_doc[keymaps['wavelength_A']]
        sample_det_distance_m = start_doc[keymaps['sample_det_distance_m']]
        beamx0 = start_doc[keymaps['beamx0']]
        beamy0 = start_doc[keymaps['beamy0']]
        # just take first detector
        first_detector = start_doc[keymaps['detectors']][0]
        detector_key = detectors2D[first_detector]['image_key']

        # look up in local library
        image_key = first_detector + "_image"
        pixel_size_um = detectors2D[first_detector]['xpixel_size_um']
        img_shape = detectors2D[first_detector]['shape']
        
        calibration = Calibration(wavelength_A=wavelength_A) # 13.5 keV
        calibration.set_image_size(img_shape[1], height=img_shape[0]) # Pilatus300k
        calibration.set_pixel_size(pixel_size_um=pixel_size_um)
        calibration.set_distance(sample_det_distance_m)
        calibration.set_beam_position(beamx0, beamy0)
      
        data = db.get_images(header, detector_key)[0]

    else:
        calibration = Calibration(wavelength_A=0.9184) # 13.5 keV
        img_shape = 619, 487
        calibration.set_image_size(487, height=619) # Pilatus300k
        calibration.set_pixel_size(pixel_size_um=172.0)
        calibration.set_distance(5.038)
        calibration.set_beam_position(263.5, 552.0)

        # make dummy data
        data = np.ones(img_shape, dtype=np.uint8)
        data[50:60] = 0
        data_filename = tmpdir_data + "/test_data.png"
        im = Image.fromarray(data)
        im.save(data_filename)


    #create dummy image and mask
    mask_data = np.ones(img_shape, dtype=np.uint8)*255
    mask_data[50:60] = 0
    mask_filename = tmpdir_analysis + "/test_mask.png"
    im = Image.fromarray(mask_data)
    im.save(mask_filename)




    #mask = Mask(mask_dir+'/Pilatus300k_main_gaps-mask.png')
    mask = Mask(mask_filename)
    mask.load(mask_filename)

    protocols = [
        #Protocols.calibration_check(show=False, AgBH=True, q0=0.010, num_rings=4, ztrim=[0.05, 0.05], ) ,
        Protocols.circular_average(ylog=True, plot_range=[0, 0.12, None, None]),
        #circular_average_q2I_fit(plot_range=[0, 0.10, 0, None]) ,
        #linecut_angle_fit(dq=0.00455*1.5) , # for q0
        #linecut_angle_fit_qm(q0=0.05, dq=0.025) , # for qm
        Protocols.thumbnails(crop=None, resize=0.5, cmap=cmap_vge_hdr, ztrim=[0.005, 0.01]) ,
        ]

    # Files to analyze
    ########################################
    root_dir = os.path.expanduser(tmpdir_data)
    source_dir = root_dir

    output_dir = tmpdir_analysis

    infiles = [data_filename]
    #infiles = glob.glob(os.path.join(source_dir, '*.tiff'))
    #infiles = glob.glob(os.path.join(source_dir, 'Ag*.tiff'))
    #infiles.sort()


    # Analysis to perform
    ########################################

    load_args = { 'calibration' : calibration,
                 'mask' : mask,
                 }
    run_args = { 'verbosity' : 3,
                 'db_analysis' : cmsdb_analysis,
                 'save_xml' : True
                }

    process = Protocols.ProcessorXS(load_args=load_args, run_args=run_args)


    # Run
    ########################################
    process.run(infiles, protocols, output_dir=output_dir, force=False)
    print("Ran successfully, now retrieving results")

    # retrieve
    fs1 = cmsdb_analysis.fs

    # can create a handler that supplies the filename
    class PNGHandlerRaw:
        def __init__(self, fpath, **kwargs):
            self.fpath = fpath

        def __call__(self, **kwargs):
            return self.fpath

    # create quick handler
    class PNGHandler:
        def __init__(self, fpath, **kwargs):
            self.fpath = fpath

        def __call__(self, **kwargs):
            return np.array(Image.open(self.fpath))

    fs1.register_handler('PNG', PNGHandlerRaw, overwrite=True)
    fs1.register_handler('JPG', PNGHandlerRaw, overwrite=True)
    fs1.register_handler('DAT', DATHandler, overwrite=True)
    fname = cmsdb_analysis.get_images(cmsdb_analysis[-1], 'thumb')[0]
    print("filename is {}".format(fname))

    fs1.register_handler('PNG', PNGHandler, overwrite=True)
    fs1.register_handler('JPG', PNGHandler, overwrite=True)
    # retrieving
    imgs = cmsdb_analysis.get_images(cmsdb_analysis[-1], 'thumb')
    print("Got an image of shape {}".format(imgs[0].shape))
    sqdat = cmsdb_analysis.get_images(cmsdb_analysis[-2], 'sqdat')
    print("Got circular average dat file of shape {}".format(sqdat[0].shape))
    sqplot = cmsdb_analysis.get_images(cmsdb_analysis[-2], 'sqplot')
    print("Got circular average plot of shape {}".format(sqplot[0].shape))
    events = list(cmsdb_analysis.get_events(cmsdb_analysis[-2], fill=True))
    print("Successfully filled events from circular average")
    events = list(cmsdb_analysis.get_events(cmsdb_analysis[-1], fill=True))
    print("Successfully filled events from thumb reduction")
    hdr_thumb = cmsdb_analysis(protocol_name="thumbnails")[0]
    print("Successfully found last run thumbnail protocol")
    hdr_thumb = cmsdb_analysis(protocol_name="thumbnails")[0]
    print("Successfully found last run circular_average protocol")
    hdr_circavg = cmsdb_analysis(protocol_name="circular_average")[0]

    return cmsdb_analysis



def test_qt(db):
    ''' requires a databroker instance'''
    from databroker_browser.qt import BrowserWindow, CrossSection, StackViewer


    search_result = lambda h: "{start[plan_name]} ['{start[uid]:.6}']".format(**h)
    text_summary = lambda h: "This is a(n) {start[plan_name]}.".format(**h)


    def fig_dispatch(header, factory):
        # plotting doesn't work for now
        '''
        print(header)
        plan_name = header['start']['plan_name']
        plotted = False
        for key in ['thumb', 'sqplot']:
            if key in header['descriptors'][0]['data_keys']:
                fig = factory('Image Series')
                cs = CrossSection(fig)
                sv = StackViewer(cs, db.get_images(header, 'thumb'))
                plotted = True
            if plotted:
                break
        '''
        pass
    
    return BrowserWindow(db, fig_dispatch, text_summary, search_result)

def test_qt_plotting(db):
    from databroker_browser.qt import BrowserWindow, CrossSection, StackViewer
    
    
    search_result = lambda h: "{start[plan_name]} ['{start[uid]:.6}']".format(**h)
    text_summary = lambda h: "This is a {start[plan_name]}.".format(**h)
    
    
    def fig_dispatch(header, factory):
        plan_name = header['start']['plan_name']
        if 'pilatus300' in header['start']['detectors']:
            fig = factory('Image Series')
            cs = CrossSection(fig)
            sv = StackViewer(cs, db.get_images(header, 'pilatus300_image'))
        elif len(header['start'].get('motors', [])) == 1:
            motor, = header['start']['motors']
            main_det, *_ = header['start']['detectors']
            fig = factory("{} vs {}".format(main_det, motor))
            ax = fig.gca()
            lp = LivePlot(main_det, motor, ax=ax)
            db.process(header, lp)
    
    
    return BrowserWindow(db, fig_dispatch, text_summary, search_result)
