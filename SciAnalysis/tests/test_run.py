import sys, os
# no need as SciAnalysis is globally installed in conda env
#SciAnalysis_PATH = os.path.expanduser("~/home/xf11bm/software/SciAnalysis")
#SciAnalysis_PATH in sys.path or sys.path.append(SciAnalysis_PATH)

import glob
from SciAnalysis import tools
from SciAnalysis.XSAnalysis.Data import *
from SciAnalysis.XSAnalysis import Protocols

import tempfile

# set up mock database
from portable_mds.sqlite.mds import MDS as MDS_Analysis
from portable_fs.sqlite.fs import FileStore as FileStore_Analysis


def test_thumb():
    # Set up of the databroker
    # This an example. You'll need to know your local configuration.
    mds_analysis_conf = {
                         'host': 'localhost',
                         'port': PORT_ANALYSIS,
                         # uses metadatastore
                         'database': 'metadatastore-production-v1',
                         'timezone': 'US/Eastern',
                         # test directory
                         'directory' : '/home/group/mongodb-cms/sqlite-db'
                         }
    mds_analysis = MDS_Analysis(mds_analysis_conf, auth=False)
    # This an example. You'll need to know your local configuration.
    fs_analysis_conf = {
                        'host': 'localhost',
                        'port': PORT_ANALYSIS,
                        'database': 'filestore-production-v1',
                        # test path
                         'dbpath' : '/home/group/mongodb-cms/sqlite-db.db'
                        }

    # if first time, run this:
    #from filestore.utils import install_sentinels
    #install_sentinels(fs_analysis_conf, version_number)
    fs_analysis = FileStore_Analysis(fs_analysis_conf)

    cmsdb_analysis = Broker(mds_analysis, fs_analysis)

    calibration = Calibration(wavelength_A=0.9184) # 13.5 keV
    calibration.set_image_size(487, height=619) # Pilatus300k
    calibration.set_pixel_size(pixel_size_um=172.0)
    calibration.set_distance(5.038)
    calibration.set_beam_position(263.5, 552.0)

    mask_dir = os.path.expanduser('~/research/projects/SciAnalysis-data/storage/masks')
    #mask = Mask(mask_dir+'/Pilatus300k_main_gaps-mask.png')
    mask = Mask(mask_dir+'/Pilatus300k_generic-mask.png')
    mask.load(mask_dir + '/Pilatus300k_generic-mask.png')

    protocols = [
        #Protocols.calibration_check(show=False, AgBH=True, q0=0.010, num_rings=4, ztrim=[0.05, 0.05], ) ,
        #Protocols.circular_average(ylog=True, plot_range=[0, 0.12, None, None]) ,
        #circular_average_q2I_fit(plot_range=[0, 0.10, 0, None]) ,
        #linecut_angle_fit(dq=0.00455*1.5) , # for q0
        #linecut_angle_fit_qm(q0=0.05, dq=0.025) , # for qm
        Protocols.thumbnails(crop=None, resize=0.5, cmap=cmap_vge_hdr, ztrim=[0.005, 0.01]) ,
        ]




    # Files to analyze
    ########################################

    root_dir = os.path.expanduser('~/research/projects/SciAnalysis-data/data')
    #root_dir = '/GPFS/xf11bm/Pilatus300/'
    #root_dir = '/GPFS/xf11bm/Pilatus300/2016-3/CFN_aligned-BCP/'


    #source_dir = os.path.join(root_dir, '')
    source_dir = root_dir


    #output_dir = os.path.join(source_dir, 'analysis/')
    #output_dir = './'
    output_dir = "../storage"

    infiles = [source_dir + "/93e70975-875f-4b57-a9c9_000000.tiff"]
    #infiles = glob.glob(os.path.join(source_dir, '*.tiff'))
    #infiles = glob.glob(os.path.join(source_dir, 'Ag*.tiff'))
    #infiles.sort()


    # Analysis to perform
    ########################################

    load_args = { 'calibration' : calibration,
                 'mask' : mask,
                 }
    run_args = { 'verbosity' : 3,
                }

    process = Protocols.ProcessorXS(load_args=load_args, run_args=run_args)


    # Run
    ########################################
    process.run(infiles, protocols, output_dir=output_dir, force=False)


    # retrieve
    from cmsdb import cmsdb_analysis
    fs1 = cmsdb_analysis.fs
    _SPEC = "PNG"
    from PIL import Image
    class PNGHandler:
        def __init__(self, fpath, **kwargs):
            self.fpath = fpath

        def __call__(self, **kwargs):
            return np.array(Image.open(self.fpath))

    # retrieving
    fs1.deregister_handler(_SPEC)
    fs1.register_handler(_SPEC, PNGHandler)
    imgs = cmsdb_analysis.get_images(cmsdb_analysis[-1], 'thumb')

    import matplotlib.pyplot as plt
    plt.ion()
    plt.imshow(imgs[0])
