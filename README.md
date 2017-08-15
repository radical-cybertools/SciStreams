# SciStreams for CMS pipeline.

## Author : Kevin G. Yager
## Ported to git by : Julien Lhermitte

This is a work in progress and a streaming version of SciAnalysis. For various
notes, see internal page:
http://gisaxs.com/CS/index.php/Databroker

INSTALL:

 * python setup.py develop

TODO:

 * current tests are not unit tests. Meant only for debugging
 * add databroker interface to all functions

File Structure:
 * core/ : The backbone of the streaming pipeline. This could likely be its own library.
    * streams.py : the streams library (from http://www.github.com/mrocklin/streams)
    * dask-streams.py : the dask extension to the streams library (from http://www.github.com/mrocklin/streams)
    * StreamDoc.py : the StreamDoc extension to the streams library
 * utils/ : various utilities that don't fit anywhere else
 * streams/ : all processes involving streams go here
 * data/ : objects that handle data
 * processing/ : functions/objects that process data
 * interfaces/ : Everything in the code runs on `StreamDoc` objects.
    External data must be converted to this format. All routines involving this
    conversion are found in folders in the `interfaces` folder:
    * /databroker : databroker data <-> StreamDoc conversions
    * /databroker_simulator : databroker data (simulated) <-> StreamDoc conversions
    * /plotting_mpl : matploblib <-> StreamDoc conversions
    * /xml : xmls <-> StreamDoc conversions
    * /file : file <-> StreamDoc conversions
    * StreamDoc.py : the `StreamDoc` object.
    * streams.py : the stream handling objects.
    * dask.py : stream handling objects involving distributed computation
 * /startup : various frontend scripts to run routine batch processing
 * /tests : unit tests 
