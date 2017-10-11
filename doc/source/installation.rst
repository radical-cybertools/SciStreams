Installation
============

The setup for SciStreams is done in a few steps

Configuration File
------------------
The configuration file is the easiest way to have `SciStreams` setup for
use. It is to be located in the home directory
`~/.config/scisteams/scistreams.yml`. The structure is as follows::

  storagedir : /path/to/storage here
  maskdir : /path/to/mask here
  delayed : True
  # client : "xx.xx.xxx.x:xxxx" (IP:HOST for client)
  # parameters for the various interfaces
  # file, xml and plotting will save to some directory given by 
  # metadata experiment_alias_directory
  resultsroot : (/old/root, /new/root)
  filestoreroot : /path/to/filestore
  # the databases used
  databases :
      cms :
          data :
              host : 'localhost'
              port : xxxxx
              mdsname : 'metadatastore-production-v1'
              fsname : "filestore-production-v1"
          analysis :
              host : 'localhost'
              port: xxxxx
              mdsname : "analysis-metadatastore-v1"
              fsname : "analysis-filestore-v1"
      chx :
          data :
              host : 'localhost'
              port : xxxxx
              mdsname : 'datastore'
              fsname : 'filestore'
  keymaps:
      cms:
          wavelength:
              name: calibration_wavelength_A
              default_value: None
              default_unit: Angstrom
          beamx0:
              name: detector_SAXS_x0_pix
              default_value: null
              default_unit: pixel
          beamy0:
              name: detector_SAXS_y0_pix
              default_value: null
              default_unit: pixel
          sample_det_distance:
              name: detector_SAXS_distance_m
              default_value: null
              default_unit: m
          pixel_size_x:
              name:
              default_value: null
              default_unit: pixel
              default_value: um
          pixel_size_y:
              name:
              default_value: null
              default_unit: pixel
              default_value: um
  required_attributes:
      # can optionally choose more categories
      main:
          # name, type
          detector_SAXS_x0_pix: number 
          detector_SAXS_y0_pix: number 
          motor_SAXSx: number
          motor_SAXSy: number
          scan_id: int
          measurement_type: str
          sample_savename: str 
          sample_name: str
          motor_bsx: number
          motor_bsy: number
          motor_bsphi: number
          detector_SAXS_distance_m: number
          calibration_energy_keV: number
          calibration_wavelength_A: number
          experiment_alias_directory: str
          experiment_cycle: str
          experiment_group: str
          filename: str
          sample_exposure_time: number
          stitchback: int


There are a few elements to this file worth mentioning.

* ``storagedir`` : the Storage directory where intermediate data can be found

* ``maskdir`` : the directory for the masks

* ``delayed`` : whether or not delayed elements are pure (True by default)

* ``client`` : the location to the dask distributed client. If not set, no
  cluster will be used.

* ``databases`` : Relevant information to set up the ``databroker`` databases

* ``keymaps`` : These are the keymaps that translate incoming metadata to key
  metadata needed for various elements in the ``SciStreams``.

* ``required_attributes`` : A dictionary outlining the general schema of the
  attributes of incoming data.

