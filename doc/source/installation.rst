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
  
