from databroker.assets.handlers import AreaDetectorTiffHandler  # noqa
from PIL import Image
import re
import os
import numpy as np
import h5py
from pims import FramesSequence, Frame


# create quick handler
class PNGHandler:
    def __init__(self, fpath, **kwargs):
        self.fpath = fpath

    def __call__(self, **kwargs):
        return np.array(Image.open(self.fpath))


# create quick handler for dicts, basically write json file
class DICTHandler:
    def __init__(self, fpath, **kwargs):
        self.fpath = fpath

    def __call__(self, **kwargs):
        return np.array(Image.open(self.fpath))

# EIGER Image reader


"""
#ref - taken from Yugang chxtools
https://github.com/yugangzhang/chxtools/blob/master/chxtools/pims_readers/eiger.py
This reader opens images taken using an Eiger detector at NSLS-II.
It expects a "master file" and a "data file" in the same directory.

It requires h5py and the hdf5-lz4 library from Dectris. The latter
has been packaged for conda and is available on the NSLS-II internal
conda channels and externally from:

conda install -c danielballan hdf5-lz4
"""


class EigerImages2(FramesSequence):
    pattern = re.compile('(.*)master.*')

    def __init__(self, master_filepath, imgthresh=None):
        # The 'master' file points to data in other files.
        # Construct a list of those filepaths and check that they exist.
        self.master_filepath = master_filepath
        self.imgthresh = imgthresh

        ndatafiles = 0
        m = self.pattern.match(os.path.basename(master_filepath))

        if m is None:
            raise ValueError("This reader expects filenames containing "
                             "the word 'master'. If the file was renamed, "
                             "revert to the original name given by the "
                             "detector.")
        prefix = m.group(1)
        pattern_data = prefix + 'data'
        head, base = os.path.split(master_filepath)
        for files in os.listdir(head):
            if pattern_data in files:
                ndatafiles += 1

        with h5py.File(master_filepath, "r") as f:
            try:
                entry = f['entry']['data']  # Eiger firmware v1.3.0 and onwards
            except KeyError:
                entry = f['entry']          # Older firmwares
            self.keys = sorted([k for k in entry.keys()
                                if k.startswith('data')])[:ndatafiles]

            lengths = [entry[key].shape[0] for key in self.keys]
        for k in self.keys:
            filename = prefix + k + '.h5'
            filepath = os.path.join(os.path.dirname(master_filepath), filename)
            if not os.path.isfile(filepath):
                raise IOError("Cannot locate expected data file: {0}".format(
                        filepath))
        # Table of Contents return a tuple:
        # self._toc[5] -> [which file, which element in that file]
        self._toc = np.concatenate(
                [list(zip(i*np.ones(length, dtype=int),
                      np.arange(length, dtype=int)))
                 for i, length in enumerate(lengths)])

        # Read in some of the detector experimental parameters
        f = h5py.File(master_filepath, "r")
        dbeam = f['entry']['instrument']['beam']
        ddet = f['entry']['instrument']['detector']
        ddetS = f['entry']['instrument']['detector']['detectorSpecific']

        self.wavelength = np.array(dbeam['incident_wavelength'])

        self.pxdimx = np.array(ddet['x_pixel_size'])
        self.pxdimy = np.array(ddet['y_pixel_size'])
        self.threshold_energy = np.array(ddet['threshold_energy'])
        self.det_distance = np.array(ddet['detector_distance'])
        self.beamx0 = np.array(ddet['beam_center_x'])
        self.beamy0 = np.array(ddet['beam_center_y'])
        self.sensor_thickness = np.array(ddet['sensor_thickness'])

        self.photon_energy = np.array(ddetS['photon_energy'])
        self.exposuretime = np.array(ddet['count_time'])
        self.timeperframe = np.array(ddet['frame_time'])
        self.nframes = np.array(ddetS['nimages'])
        self.version = np.array(ddetS['software_version'])
        self.date = np.array(ddetS['data_collection_date'])
        dimx = np.array(ddetS['x_pixels_in_detector'])
        dimy = np.array(ddetS['y_pixels_in_detector'])
        # dims from reader are flipped so I keep this notation (matrix indexing
        # versus image indexing)
        self.dims = (dimy, dimx)

        f.close()

        # Quick check for version change, if not a tested version, warn user.
        if((self.version != b'1.3.0')*(self.version != b'1.5.0')):
            errormsg = "Warning, this has only been tested on EIGER"
            errormsg += "version(s) 1.3.0\n\ "
            errormsg += "Your current version is {}.".format(self.version)
            errormsg += "Please ask beamline staff to verify the \n"
            errormsg += "version difference will not affect your reading."
            print(errormsg)

    def get_frame(self, i):
        key_number, elem_number = self._toc[i]
        key = self.keys[key_number]
        with h5py.File(self.master_filepath, "r") as f:
            try:
                # Eiger firmware v1.3.0 and onwards
                img = f['entry']['data'][key][elem_number]
            except KeyError:
                img = f['entry'][key][elem_number]          # Older firmwares
        if(self.imgthresh is not None):
            img *= (img < self.imgthresh)
        return Frame(img, frame_no=i)

    # def get_avg(self,frms=None):

    def get_flatfield(self):
        '''EIGER specific routine to obtain the flatfield correction.'''
        f = h5py.File(self.master_filepath, "r")
        ddetS = f['entry']['instrument']['detector']['detectorSpecific']
        flatfield = np.array(ddetS['flatfield'])
        f.close()
        return flatfield

    def get_pixel_mask(self):
        ''' Get the pixel mask reported by the EIGER.'''
        f = h5py.File(self.master_filepath, "r")
        ddetS = f['entry']['instrument']['detector']['detectorSpecific']
        pxmsk = np.array(ddetS['pixel_mask'])
        f.close()
        return pxmsk

    def __len__(self):
        return len(self._toc)

    @property
    def frame_shape(self):
        return self[0].shape

    @property
    def pixel_type(self):
        return self[0].dtype


# Another Eiger Image reader
"""
#ref - taken from Yugang chxtools
https://github.com/yugangzhang/chxtools/blob/master/chxtools/pims_readers/eiger.py
This reader opens images taken using an Eiger detector at NSLS-II.
It expects a "master file" and a "data file" in the same directory.

It requires h5py and the hdf5-lz4 library from Dectris. The latter
has been packaged for conda and is available on the NSLS-II internal
conda channels and externally from:

conda install -c danielballan hdf5-lz4
"""


class EigerImages(FramesSequence):
    pattern = re.compile('(.*)master.*')

    def __init__(self, master_filepath):
        # The 'master' file points to data in other files.
        # Construct a list of those filepaths and check that they exist.
        self.master_filepath = master_filepath

        ndatafiles = 0
        m = self.pattern.match(os.path.basename(master_filepath))

        if m is None:
            raise ValueError("This reader expects filenames containing "
                             "the word 'master'. If the file was renamed, "
                             "revert to the original name given by the "
                             "detector.")
        prefix = m.group(1)
        pattern_data = prefix + 'data'
        head, base = os.path.split(master_filepath)
        for files in os.listdir(head):
            if pattern_data in files:
                ndatafiles += 1

        with h5py.File(master_filepath, "r") as f:
            try:
                entry = f['entry']['data']  # Eiger firmware v1.3.0 and onwards
            except KeyError:
                entry = f['entry']          # Older firmwares
            self.keys = sorted([k for k in entry.keys()
                                if k.startswith('data')])[:ndatafiles]

            lengths = [entry[key].shape[0] for key in self.keys]

        for k in self.keys:
            filename = prefix + k + '.h5'
            filepath = os.path.join(os.path.dirname(master_filepath), filename)
            if not os.path.isfile(filepath):
                raise IOError("Cannot locate expected data file: {0}".format(
                        filepath))
        # Table of Contents return a tuple:
        # self._toc[5] -> [which file, which element in that file]
        self._toc = np.concatenate(
                [list(zip(i*np.ones(length, dtype=int),
                      np.arange(length, dtype=int)))
                 for i, length in enumerate(lengths)])

        # Read in some of the detector experimental parameters
        f = h5py.File(master_filepath, "r")
        dbeam = f['entry']['instrument']['beam']
        ddet = f['entry']['instrument']['detector']
        ddetS = f['entry']['instrument']['detector']['detectorSpecific']

        self.wavelength = np.array(dbeam['incident_wavelength'])

        self.pxdimx = np.array(ddet['x_pixel_size'])
        self.pxdimy = np.array(ddet['y_pixel_size'])
        self.threshold_energy = np.array(ddet['threshold_energy'])
        self.det_distance = np.array(ddet['detector_distance'])
        self.beamx0 = np.array(ddet['beam_center_x'])
        self.beamy0 = np.array(ddet['beam_center_y'])
        self.sensor_thickness = np.array(ddet['sensor_thickness'])

        self.photon_energy = np.array(ddetS['photon_energy'])
        self.exposuretime = np.array(ddet['count_time'])
        self.timeperframe = np.array(ddet['frame_time'])
        self.nframes = np.array(ddetS['nimages'])
        self.version = np.array(ddetS['software_version'])
        self.date = np.array(ddetS['data_collection_date'])
        dimx = np.array(ddetS['x_pixels_in_detector'])
        dimy = np.array(ddetS['y_pixels_in_detector'])
        # dims from reader are flipped so I keep this notation (matrix indexing
        # versus image indexing)
        self.dims = (dimy, dimx)

        f.close()

        # Quick check for version change, if not a tested version, warn user.
        if((self.version != b'1.3.0')*(self.version != b'1.5.0')):
            errormsg = "Warning, this has only been tested on EIGER"
            errormsg += "version(s) 1.3.0 and 1.5.0\n\ "
            errormsg += "Your current version is {}.".format(self.version)
            errormsg += "Please ask beamline staff to verify the \n"
            errormsg += "version difference will not affect your reading."
            print(errormsg)

    def get_frame(self, i):
        key_number, elem_number = self._toc[i]
        key = self.keys[key_number]
        with h5py.File(self.master_filepath, "r") as f:
            try:
                # Eiger firmware v1.3.0 and onwards
                img = f['entry']['data'][key][elem_number]
            except KeyError:
                # Older firmwares
                img = f['entry'][key][elem_number]
        return Frame(img, frame_no=i)

    # def get_avg(self,frms=None):

    def get_flatfield(self):
        '''EIGER specific routine to obtain the flatfield correction.'''
        f = h5py.File(self.master_filepath, "r")
        ddetS = f['entry']['instrument']['detector']['detectorSpecific']
        flatfield = np.array(ddetS['flatfield'])
        f.close()
        return flatfield

    def get_pixel_mask(self):
        ''' Get the pixel mask reported by the EIGER.'''
        f = h5py.File(self.master_filepath, "r")
        ddetS = f['entry']['instrument']['detector']['detectorSpecific']
        pxmsk = np.array(ddetS['pixel_mask'])
        f.close()
        return pxmsk

    def __len__(self):
        return len(self._toc)

    @property
    def frame_shape(self):
        return self[0].shape

    @property
    def pixel_type(self):
        return self[0].dtype


# Eiger HDF5 header information
# wavelength:
#  /entry/instrument/beam/incident_wavelength
# energy:
#  /entry/instrument/detector/detectorSpecific/photon_energy
# beam x and y centers:
#  /entry/instrument/detector/beam_center_x
#  /entry/instrument/detector/beam_center_y
# sample detector distance:
#  /entry/instrument/detector/detector_distance
# Make sure threshold energy is set to half the energy of the incoming photons:
#  /entry/instrument/detector/threshold_energy
