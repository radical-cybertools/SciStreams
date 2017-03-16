#!/usr/bin/python
# -*- coding: utf-8 -*-
# vi: ts=4 sw=4
'''
:mod:`SciAnalysis.tools` - Helpful tools
================================================
.. module:: SciAnalysis.tools
   :synopsis: Provides small tools helpful in different contexts
.. moduleauthor:: Dr. Kevin G. Yager <kyager@bnl.gov>
                    Brookhaven National Laboratory
'''

################################################################################
#  Small tools.
################################################################################
# Known Bugs:
#  N/A
################################################################################
# TODO:
#  Search for "TODO" below.
################################################################################

import os
import time
#import xml.etree.ElementTree as etree # XML read/write
from lxml import etree
import xml.dom.minidom as minidom
# import the analysis databroker
from uuid import uuid4
import numpy as np

_ANALYSIS_STORE_VERSION = 'beta-v1'


def make_dir(directory):
    if not os.path.isdir(directory):
        #os.mkdir( directory )
        os.makedirs( directory )

def timestamp(filepath):
    statinfo = os.stat(filepath)
    filetimestamp = statinfo.st_mtime
    return filetimestamp


# Filename
################################################################################
class Filename(object):
    '''Parses a filename into pieces following the desired pattern.'''

    def __init__(self, filepath):
        '''Creates a new Filename object, to make it easy to separate the filename
        into its pieces (path, file, extension).'''

        self.full_filepath = filepath
        self._update()


    def _update(self):

        path, filename, filebase, ext = self.file_split(self.full_filepath)

        self.path = path            # filesystem path to file
        self.filename = filename    # filename (including extension)
        self.filebase = filebase    # filename (without extension)
        self.ext = ext              # extension


    def file_split(self, filepath):


        filepath, filename = os.path.split(filepath)
        filebase, ext = os.path.splitext(filename)

        return filepath, filename, filebase, ext


    def split(self):
        # filepath, filename, filebase, ext = f.split()
        return self.path, self.filename, self.filebase, self.ext

    def get_filepath(self):
        return self.full_filepath

    def get_path(self):
        return self.path+'/'

    def get_filename(self):
        return self.filename

    def get_filebase(self):
        return self.filebase

    def get_ext(self):
        return self.ext

    def timestamp(self):
        statinfo = os.stat(self.full_filepath)
        filetimestamp = statinfo.st_mtime
        return filetimestamp

    def matches_basename(self, filepath):
        path, filename, filebase, ext = self.file_split(filepath)

        return self.filebase==filebase

    def append(self, text):
        self.full_filepath = os.path.join(self.path, self.filebase + text + self.ext)
        self._update()

    # End class Filename(object)
    ########################################





# Processor
################################################################################
class Processor(object):
    '''Base class for processing a bunch of data files.'''
    def __init__(self, load_args={}, run_args={}):
        ''' Initialize the processor.
            
            Parameters
            ----------

            load_args : 

            run_args : 
                Run arguments. Can contain (but not limited to):

                save_xml : bool
                    If true, processor will save xml files with results
    
                db_analysis : databroker instance
                    If set, then processor will save to this databroker instance
            
        '''

        # xml saving setup
        if 'save_xml' in run_args:
            self.save_xml = run_args['save_xml']
        else:
            run_args['save_xml'] = True

        # databroker setup
        if 'db_analysis' in run_args:
            self.db_analysis = run_args['db_analysis']
        else:
            run_args['db_analysis'] = None

        self.load_args = load_args
        self.run_args = run_args


    def set_files(self, infiles):

        self.infiles = infiles


    def set_protocols(self, protocols):

        self.protocols = protocols


    def set_output_dir(self, output_dir):

        self.output_dir = output_dir

    def access_dir(self, base, extra=''):
        '''Returns a string which is the desired output directory.
        Creates the directory if it doesn't exist.'''

        output_dir = os.path.join(base, extra)
        make_dir(output_dir)

        return output_dir


    def run(self, infiles=None, protocols=None, output_dir=None, force=False,
            ignore_errors=False, sort=False, load_args={}, run_args={},
            **kwargs):
        '''Process the specified files using the specified protocols.'''

        l_args = self.load_args.copy()
        l_args.update(load_args)
        r_args = self.run_args.copy()
        r_args.update(run_args)
        if self.run_args['db_analysis'] is not None:
            r_args['db_analysis'] = self.run_args['db_analysis']
        else:
            r_args['db_analysis'] = None

        if infiles is None:
            infiles = self.infiles
        if sort:
            infiles.sort()

        if protocols is None:
            protocols = self.protocols

        if output_dir is None:
            output_dir = self.output_dir


        for infile in infiles:

            try:
                data = self.load(infile, **l_args)

                for protocol in protocols:

                    output_dir_current = self.access_dir(output_dir, protocol.name)

                    if not force and protocol.output_exists(data.name, output_dir_current):
                        # Data already exists
                        print('Skipping {} for {}'.format(protocol.name, data.name))

                    else:
                        print('Running {} for {}'.format(protocol.name, data.name))

                        results = protocol.run(data, output_dir_current, **r_args)

                        self.store_results(results, output_dir, infile, protocol)

            except OSError:
                print('  ERROR (OSError) with file {}.'.format(infile))

            # NOTE : Uncomment this for final version
            #except Exception as exception:
                # Ignore errors, so that execution doesn't get stuck on a single bad file
                #print('  ERROR ({}) with file {}.'.format(exception.__class__.__name__, infile))


    def load(self, infile, **kwargs):

        data = Data2D(infile, **kwargs)

        return data


    def store_results(self, results, output_dir, name, protocol):
        ''' This function stores the results from the analysis (run in the
        `run` method) It currently writes to both xml and also databroker.
        '''
        attributes = {}
        attributes['name'] = protocol.name
        # TODO : added this, I think "protocol_name" may be more precise for metadata
        # okay to remove 'name'?
        attributes['protocol_name'] = protocol.name
        attributes['start_timestamp'] = protocol.start_timestamp
        attributes['end_timestamp'] = protocol.end_timestamp
        attributes['runtime'] = protocol.end_timestamp - protocol.start_timestamp
        attributes['save_timestamp'] = time.time()
        attributes['output_dir'] = output_dir
        attributes = dict([k, str(v)] for k, v in attributes.items())

        # save_xml needs to be first. if saved, it modifies attributes
        # (adds xml filename)
        if self.run_args['save_xml']:
            self.store_results_xml(results, attributes, output_dir, name, protocol)

        if self.run_args['db_analysis'] is not None:
            self.store_results_databroker(results, attributes, 
                                          name, protocol, self.run_args['db_analysis'])


    def store_results_xml(self, results, attributes, output_dir, name, protocol):
        output_dir = self.access_dir(output_dir, 'results')
        outfile = os.path.join(output_dir, Filename(name).get_filebase()+'.xml' )
        attributes['outfile'] = outfile

        if os.path.isfile(outfile):
            # Result XML file already exists
            parser = etree.XMLParser(remove_blank_text=True)
            root = etree.parse(outfile, parser).getroot()

        else:
            # Create new XML file
            # TODO: Add characteristics of outfile
            root = etree.Element('DataFile', name=name)

        prot = etree.SubElement(root, 'protocol', **attributes)

        # Saving to xml
        for name, content in results.items():
            if name[0] == '_':
                continue  # ignore hidden variables, like _start etc
            import numpy as np

            if isinstance(content, dict):
                content = dict([k, str(v)] for k, v in content.items())
                etree.SubElement(prot, 'result', name=name, **content)

            elif isinstance(content, list) or isinstance(content, np.ndarray):

                res = etree.SubElement(prot, 'result', name=name, type='list')
                for i, element in enumerate(content):
                    etree.SubElement(res, 'element', index=str(i), value=str(element))

            else:
                etree.SubElement(prot, 'result', name=name, value=str(content))

        tree = etree.ElementTree(root)
        tree.write(outfile, pretty_print=True)

    def store_results_databroker(self, results, attributes, name, protocol,
                                 db):
        ''' Save results to a databroker instance.'''
        # saving to databroker
        mds = db.mds # metadatastore

        # Store in databroker, make the documents
        start_doc = dict()

        start_doc.update(attributes)

        start_doc['time'] = time.time()
        start_doc['uid'] = str(uuid4())
        start_doc['plan_name'] = 'analysis'
        start_doc['name'] = protocol.name
        start_doc['start_timestamp'] = protocol.start_timestamp
        start_doc['end_timestamp'] = protocol.end_timestamp
        start_doc['runtime'] = protocol.end_timestamp - protocol.start_timestamp
        start_doc['save_timestamp'] = time.time()
        start_doc['analysis_store_version'] = _ANALYSIS_STORE_VERSION

        if '_run_args' in results:
            parse_args(results['_run_args'])
            start_doc['run_args'] = results['_run_args']

        # just make one descriptor and event document for now
        # initialize both event and descriptor
        descriptor_doc = dict()
        event_doc = dict()
        event_doc['data'] = dict()
        event_doc['timestamps'] = dict()
        descriptor_doc['data_keys'] = dict()
        descriptor_doc['time'] = time.time()
        descriptor_doc['uid'] = str(uuid4())
        descriptor_doc['run_start'] = start_doc['uid']
        event_doc['time'] = time.time()
        event_doc['uid'] = str(uuid4())
        event_doc['descriptor'] = descriptor_doc['uid']
        event_doc['seq_num'] = 1

        # first parse data
        for key, val in results.items():
            if key[0] == '_':
                continue # ignore hidden keys
            # guess descriptor from data
            descriptor_doc['data_keys'][key] = make_descriptor(val)
            event_doc['data'][key] = val
            event_doc['timestamps'][key] = time.time()

        # then parse files, val is a dict
        for key, val in results['_files'].items():
            datum, desc = parse_file_event(val, db)
            descriptor_doc['data_keys'][key] = desc
            event_doc['data'][key] = datum
            event_doc['timestamps'][key] = time.time()


        stop_doc = dict()
        stop_doc['time'] = time.time()
        stop_doc['uid'] = str(uuid4())
        stop_doc['run_start'] = start_doc['uid']
        stop_doc['exit_status'] = 'success'

        mds.insert('start', start_doc)
        mds.insert('descriptor', descriptor_doc)
        mds.insert('event', event_doc)
        mds.insert('stop', stop_doc)




    def rundirs(self, indir, pattern='*', protocols=None, output_dir=None, force=False, check_timestamp=False, ignore_errors=False, sort=True, load_args={}, run_args={}, **kwargs):

        import glob

        dirs = [name for name in os.listdir(indir) if os.path.isdir(os.path.join(indir, name))]

        if sort:
            dirs.sort()


        for directory in dirs:
            print('Running directory {}'.format(directory))

            #infiles = glob.glob('{}/{}'.format(os.path.join(indir, directory), pattern))
            infiles = glob.glob(os.path.join(indir, directory, pattern))

            output_dir_current = os.path.join(output_dir, directory)

            self.run(infiles=infiles, protocols=protocols, output_dir=output_dir_current, force=force, ignore_errors=ignore_errors, sort=sort, load_args=load_args, run_args=run_args, **kwargs)



    def run_alternate_inner(self, infiles=None, protocols=None, output_dir=None, force=False, ignore_errors=False, sort=False, load_args={}, run_args={}, **kwargs):
        '''Process the specified files using the specified protocols.
        This version defers loading data until necessary. If running multiple
        protocols, the data is reloaded many times (inefficient), but if
        running on a directory with most data already processed, this
        avoids useless loads.'''

        l_args = self.load_args.copy()
        l_args.update(load_args)
        r_args = self.run_args.copy()
        r_args.update(run_args)

        if infiles is None:
            infiles = self.infiles
        if sort:
            infiles.sort()

        if protocols is None:
            protocols = self.protocols

        if output_dir is None:
            output_dir = self.output_dir


        for infile in infiles:

            try:

                data_name = Filename(infile).get_filebase()

                for protocol in protocols:

                    output_dir_current = self.access_dir(output_dir, protocol.name)

                    if not force and protocol.output_exists(data_name, output_dir_current):
                        # Data already exists
                        print('Skipping {} for {}'.format(protocol.name, data_name))

                    else:
                        data = self.load(infile, **l_args)

                        print('Running {} for {}'.format(protocol.name, data.name))

                        results = protocol.run(data, output_dir_current, **r_args)

                        self.store_results(results, output_dir, infile, protocol)

            except (OSError, ValueError):
                print('  ERROR with file {}.'.format(infile))



    # class Processor(object)
    ########################################



# Protocol
################################################################################
def run_default(inner_function):
    '''Standard book-keeping required for the 'run' method of any protocol.'''
    def _run_default(self, *args, **kwargs):

        run_args = self.run_args.copy()
        run_args.update(kwargs)

        self.ir = 1
        self.start_timestamp = time.time()

        results = inner_function(self, *args, **run_args)

        self.end_timestamp = time.time()

        return results

    return _run_default

class Protocol(object):
    '''Base class for defining an analysis protocol, which can be applied to
    data.'''

    def __init__(self, name=None, **kwargs):

        self.name = self.__class__.__name__ if name is None else name

        self.default_ext = '.out'
        self.run_args = {}
        self.run_args.update(kwargs)


    def get_outfile(self, name, output_dir, ext=None, ir=False):

        if ext is None:
            ext = self.default_ext

        if ir:
            name = '{:02d}_{}{}'.format(self.ir, name, ext)
            self.ir += 1
        else:
            name = name + ext

        return os.path.join(output_dir, name)


    def output_exists(self, name, output_dir):

        if 'file_extension' in self.run_args:
            ext = self.run_args['file_extension']
        else:
            ext = None

        outfile = self.get_outfile(name, output_dir, ext=ext)
        return os.path.isfile(outfile)


    def prepend_keys(self, dictionary, prepend):

        new_dictionary = {}
        for key, value in dictionary.items():
            new_dictionary['{}{}'.format(prepend,key)] = value

        return new_dictionary


    @run_default
    def run(self, data, output_dir, **run_args):

        outfile = self.get_outfile(data.name, output_dir)

        results = {}

        return results



    # End class Protocol(object)
    ########################################



# get_result
################################################################################
def get_result_xml(infile, protocol):
    '''Extracts a list of results for the given protocl, from the specified
    xml file. The most recent run of the protocol is used.'''

    import numpy as np
    #import xml.etree.ElementTree as etree
    from lxml import etree
    #import xml.dom.minidom as minidom

    parser = etree.XMLParser(remove_blank_text=True)
    root = etree.parse(infile, parser).getroot()

    # Get the latest protocol
    element = root
    children = [child for child in element if child.tag=='protocol' and child.get('name')==protocol]
    children_v = [float(child.get('end_timestamp')) for child in element if child.tag=='protocol' and child.get('name')==protocol]

    idx = np.argmax(children_v)
    protocol = children[idx]

    # In this protocol, get all the results (in order)
    element = protocol
    children = [child for child in element if child.tag=='result']
    children_v = [child.get('name') for child in element if child.tag=='result']

    idx = np.argsort(children_v)
    #result_elements = np.asarray(children)[idx]
    result_elements = [children[i] for i in idx]

    results = {}
    for element in result_elements:

        #print( element.get('name') )

        if element.get('value') is not None:
            results[element.get('name')] = float(element.get('value'))

            if element.get('error') is not None:
                results[element.get('name')+'_error'] = float(element.get('error'))

        elif element.get('type') is not None and element.get('type')=='list':

            # Elements of the list
            children = [child for child in element if child.tag=='element']
            children_v = [int(child.get('index')) for child in element if child.tag=='element']
            #print(children_v)

            # Sorted
            idx = np.argsort(children_v)
            children = [children[i] for i in idx]

            # Append values
            for child in children:
                #print( child.get('index') )
                name = '{}_{}'.format(element.get('name'), child.get('index'))
                results[name] = float(child.get('value'))


        else:
            print('    Errror: result has no usable data ({})'.format(element))


    return results

    # End def get_result()
    ########################################

# Databroker tools
##################################################################
# add to results for filestore to handle
# see saveschematic.txt for deails

def parse_args(argsdict):
    ''' Parse args and make sure they are databroker friendly.
        Also useful for hashing the args.
        For ex: if it's a matplotlib instance etc just ignore

        Modifies in place.

        Warning : This is a recursive function
    '''
    for key, val in argsdict.items():
        if isinstance(val, dict):
            parse_args(val)
        elif isinstance(val, np.ndarray):
            # do nothing
            pass
        elif np.isscalar(val):
            # convenient to check if it's a number
            pass
        else:
            argsdict[key] = str(type(val))
            

def make_descriptor(val):
    ''' make a descriptor from value through guessing.'''
    shape = ()
    if np.isscalar(val):
        dtype = 'number'
    elif isinstance(val, np.ndarray):
        dtype = 'array'
        shape = val.shape
    elif isinstance(val, list):
        dtype = 'list'
        shape = (len(val),)
    elif isinstance(val, dict):
        dtype = 'dict'
    else:
        dtype = 'unknown'

    return dict(dtype=dtype, shape=shape)

    
def parse_file_event(entry, db):
    ''' Parse a file event descriptor (our custom descriptor),
        and translate into a datum (could be uid, or actual data)
        and a datum_dict (dictionary descriptor for the datum)

        Returns
        -------
        datum : the result 
        datum_dict : the dictionary describing the result
    '''
    dat_dict = dict()
    if 'dtype' in entry:
        dat_dict['dtype'] = entry['dtype']
    if 'shape' in entry:
        dat_dict['shape'] = entry['shape']
    if 'source' in entry:
        dat_dict['source'] = entry['source']
    if 'external' in entry:
        dat_dict['external'] = entry['external']
    if 'filename' in entry:
        dat_dict['filename'] = entry['filename']

    # this is for filestore instance
    if 'filename' in entry:
        fs = db.fs # get filestore
        # make sure it's absolute path
        filename = os.path.abspath(os.path.expanduser(entry['filename']))
        dat_uid = str(uuid4())
        # try to guess some parameters here
        if 'spec' in entry:
            spec = entry['spec']
        else:
            extension = os.path.splitext(filename)[1]
            if len(extension) > 1:
                spec = extension[1:].upper()
            else:
                raise ValueError("Error could not figure out file type for {}".format(filename))
        entry.setdefault('resource_kwargs', {})
        entry.setdefault('datum_kwargs', {})
        resource_kwargs = entry['resource_kwargs']
        datum_kwargs = entry['datum_kwargs']
        # could also add datum_kwargs
        # databroker : two step process: 1. insert resource 2. Save data
        resource_document = fs.insert_resource(spec, filename, resource_kwargs)
        fs.insert_datum(resource_document, dat_uid, datum_kwargs)
        # overwrite with correct argument
        dat_dict['external'] = "FILESTORE:"

    return dat_uid, dat_dict


# Notes
################################################################################
# verbosity=0 : Output nothing
# verbosity=1 : Output only final (minimal) result
# verbosity=2 : Output 'regular' amounts of information/data
# verbosity=3 : Output all useful results
# verbosity=4 : Output marginally useful things (e.g. essentially redundant/obvious things)
# verbosity=5 : Output everything (e.g. for testing)
