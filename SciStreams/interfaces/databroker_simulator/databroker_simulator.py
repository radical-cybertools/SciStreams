# Databroker tools
##################################################################
# add to results for filestore to handle
# see saveschematic.txt for deails
from uuid import uuid4
import numpy as np
import matplotlib

from ..StreamDoc import StreamDoc
# TODO : Replace this with more complex image simulators
from .generate_images import gen_image
from ..detectors import detectors2D

matplotlib.use("Agg")


'''
    Simulate the databroker data. Just return a streamdoc
'''


def pullfromuid(uid, dbname=None):
    ''' This is a hard-coded simulator that returns a simulated image from a
    pilatus detector.
        Ignores the incoming parameters

        Parameters
        ----------

        dbname : str
            Input name is of format "dbname:subdbname"
            For example : "cms:data", or "cms:analysis"
            if just database name supplied, then it's assumed
            to be analysis.

        uid : the uid of dataset

        Returns
        -------
        StreamDoc of data

    '''
    if dbname is None:
        raise ValueError("Error must supply a dbname")

    if uid is None:
        raise ValueError("Need to specify a uid")

    detkey = 'pilatus300'
    shape = detectors2D[detkey]['shape']['value']
    phi = np.random.random()*2*np.pi
    # make it at least 1
    A = (1-np.random.random()*.5)*200 + 101
    syms = np.arange(0, 11, 2)
    sym = np.random.permutation(syms)[0]
    origin = np.random.random()*shape[0], np.random.random()*shape[1]

    img = gen_image(origin=origin, A=A, sym=sym, phi=phi, shape=shape)

    kwargs = {detkey: img}

    # These were taken from cmsdb, modified a little
    attr = {
            'beam_divergence_x_mrad': 0.099988,
            'beam_divergence_y_mrad': 0.099999,
            'beam_intensity_expected': 8300,
            'beam_size_x_mm': 0.19999050000000018,
            'beam_size_y_mm': 0.19999725000000002,
            'beamline_mode': 'undefined',
            'calibration_energy_keV': 13.5,
            'calibration_wavelength_A': 0.91839,
            'detector_SAXS_distance_m': 3,
            'detector_SAXS_name': 'pilatus300',
            'detector_SAXS_x0_pix': origin[1],
            'detector_SAXS_y0_pix': origin[0],
            'detector_sequence_ID': 0,
            'detectors': [detkey],
            'detector_name': detkey,
            'experiment_SAF_number': 'N/A',
            'experiment_alias_directory': 'test-data',
            'experiment_cycle': 'test-cycle',
            'experiment_group': 'beamline',
            'experiment_project': 'SAXS',
            'experiment_proposal_number': 'N/A',
            'experiment_type': 'SAXS',
            'experiment_user': 'various',
            'filename': 'test-data-filename.tiff',
            'measure_series_motor': 'srot',
            'measure_series_num_frames': 3,
            'measure_series_positions': [-1, 1],
            'measure_type': 'measureScanSeries',
            'motor_DETx': 0.0,
            'motor_DETy': -16.0,
            'motor_SAXSx': -1.1200000074040872e-06,
            'motor_SAXSy': 35.00005232,
            'motor_WAXSx': -16.9,
            'motor_bsphi': -11.95037499999998,
            'motor_bsx': -16.157401999999998,
            'motor_bsy': -14.708893,
            'motor_smx': -17.15005,
            'motor_smy': -1.8,
            'motor_sth': 0.0,
            'motors': ['srot'],
            'num_intervals': 3,
            'num_points': 4,
            'sample_clock': 175.1852946281433,
            'sample_exposure_time': 1,
            'sample_measurement_ID': 4,
            'sample_motor_phi': -0.00038000000000693035,
            'sample_motor_th': 0.0,
            'sample_motor_trans2': 0.999999,
            'sample_motor_x': -17.15005,
            'sample_motor_y': -1.8,
            'sample_name': 'test',
            'sample_phi': -0.00038000000000693035,
            'sample_savename': 'test-data',
            'sample_savename_extra': None,
            'sample_temperature': -273.15,
            'sample_th': 0.0,
            'sample_trans2': 3.5989729999999995,
            'sample_x': -17.15005,
            'sample_y': -1.8,
            'scan': 'scan_measure',
            'scan_id': 99372,
            'stitchback': False,
            'time': 1501616740.0414796,
            'uid': str(uuid4())}

    sdoc = StreamDoc()
    sdoc.add(kwargs=kwargs)
    sdoc.add(attributes=attr)

    return sdoc


def pullfromuids(dbname, uids):
    for uid in uids:
        yield pullfromuid(dbname, uid)
