# doesn't need to be an object
import os.path
import numpy as np
from lxml import etree
import json

from ... import config
_ROOTDIR = config.resultsroot
_ROOTMAP = config.resultsrootmap


def make_dir(directory):
    ''' Creates directory if doesn't exist.'''
    if not os.path.isdir(directory):
        os.makedirs(directory)


def access_dir(base, extra=''):
    '''Returns a string which is the desired output directory.
    Creates the directory if it doesn't exist.'''

    output_dir = os.path.join(base, extra)
    make_dir(output_dir)

    return output_dir


def get_filebase(name):
    basename = os.path.basename(name)
    basename, ext = os.path.splitext(basename)
    return basename


def parse_attrs_xml(attrs):
    ''' parse the attributes into a string for each element'''
    new_attrs = dict()
    for key, val in attrs.items():
        if isinstance(val, dict):
            new_attrs[key] = json.dumps(val)
        else:
            new_attrs[key] = str(val)
    return new_attrs


def _cleanup_str(string):
    string = string.replace(" ", "_")
    string = string.replace("/", "_")
    string = string.replace("(", "_")
    string = string.replace(")", "_")
    string = string.replace(":", "_")
    return string


def _make_fname_from_attrs(attrs):
    ''' make filename from attributes.
        This will likely be copied among a few interfaces.
    '''
    if 'experiment_alias_directory' not in attrs:
        raise ValueError("Error cannot find experiment_alias_directory" +
                         " in attributes. Not saving.")

    # remove the trailing slash
    rootdir = attrs['experiment_alias_directory'].strip("/")

    if _ROOTMAP is not None:
        rootdir = rootdir.replace(_ROOTMAP[0], _ROOTMAP[1])
    elif _ROOTDIR is not None:
        rootdir = _ROOTDIR

    if 'detector_name' not in attrs:
        raise ValueError("Error cannot find detector_name in attributes")
    else:
        detname = _cleanup_str(attrs['detector_name'])
        # get name from lookup table first
        detector_name = config.detector_names.get(detname, detname)

    if 'sample_savename' not in attrs:
        raise ValueError("Error cannot find sample_savename in attributes")
    else:
        sample_savename = _cleanup_str(attrs['sample_savename'])

    if 'stream_name' not in attrs:
        # raise ValueError("Error cannot find stream_name in attributes")
        stream_name = 'unnamed_analysis'
    else:
        stream_name = _cleanup_str(attrs['stream_name'])

    if 'scan_id' not in attrs:
        raise ValueError("Error cannot find scan_id in attributes")
    else:
        scan_id = _cleanup_str(str(attrs['scan_id']))

    outdir = rootdir + "/" + detector_name + "/" + stream_name + "/xml"
    make_dir(outdir)
    outfile = outdir + "/" + sample_savename + "_" + scan_id + ".xml"

    return outfile


def store_results_xml(results, outputs=None):
    '''
        Store the results from the corresponding protocol.

        Parameters
        ----------

        results : a SciResult object

        expects a experiment_cycle, experiment group and sample_savename
        path is ROOT/experiment_cycle/experiment_group/sample_savename.xml

    '''
    # TODO : maybe add date folder too?
    # TODO : add detector as well?
    if 'kwargs' not in results:
        raise ValueError("kwargs not in the sciresults. " +
                         "(Is this a valid SciResult object?)")
    results_dict = results['kwargs']
    if 'attributes' not in results:
        raise ValueError("attributes not in the sciresults. " +
                         "(Is this a valid SciResult object?)")

    attrs = results['attributes']
    outfile = _make_fname_from_attrs(attrs)

    # just add to the sciresults so user knows it's been saved to xml
    results['attributes']['xml-outfile'] = outfile

    if os.path.isfile(outfile):
        # Result XML file already exists
        parser = etree.XMLParser(remove_blank_text=True)
        root = etree.parse(outfile, parser).getroot()
    else:
        # Create new XML file
        # TODO: Add characteristics of outfile
        if 'sample_savename' in attrs:
            sample_savename = attrs['sample_savename']
        else:
            sample_savename = "No sample savename"
        root = etree.Element('DataFile', name=sample_savename)

    # TODO : instead of parsing (changing to str), walk through all elements in
    # tree of dicts
    attrs_parsed = parse_attrs_xml(attrs)
    prot = etree.SubElement(root, 'protocol', **attrs_parsed)

    # Saving to xml
    if outputs is not None:
        for output in outputs:
            name = output
            content = results_dict[name]
            if name[0] == '_':
                continue  # ignore hidden variables, like _start etc

            if isinstance(content, dict):
                content = dict([k, str(v)] for k, v in content.items())
                etree.SubElement(prot, 'result', name=name, **content)

            elif isinstance(content, list) or isinstance(content, np.ndarray):

                res = etree.SubElement(prot, 'result', name=name, type='list')
                for i, element in enumerate(content):
                    etree.SubElement(res, 'element', index=str(i),
                                     value=str(element))

            else:
                etree.SubElement(prot, 'result', name=name, value=str(content))

    tree = etree.ElementTree(root)
    tree.write(outfile, pretty_print=True)


def get_result(infile, protocol):
    '''Extracts a list of results for the given protocol, from the specified
    xml file. The most recent run of the protocol is used.'''
    # NOTE : Not tested yet

    parser = etree.XMLParser(remove_blank_text=True)
    root = etree.parse(infile, parser).getroot()

    # Get the latest protocol
    element = root
    children = [child for child in element
                if child.tag == 'protocol' and child.get('name') == protocol]
    children_v = [float(child.get('end_timestamp'))
                  for child in element
                  if child.tag == 'protocol' and child.get('name') == protocol]

    idx = np.argmax(children_v)
    protocol = children[idx]

    # In this protocol, get all the results (in order)
    element = protocol
    children = [child for child in element if child.tag == 'result']
    children_v = [child.get('name')
                  for child in element if child.tag == 'result']

    idx = np.argsort(children_v)
    # result_elements = np.asarray(children)[idx]
    result_elements = [children[i] for i in idx]

    results = {}
    for element in result_elements:

        # print( element.get('name') )

        if element.get('value') is not None:
            results[element.get('name')] = float(element.get('value'))

            if element.get('error') is not None:
                results[element.get('name')+'_error'] = \
                    float(element.get('error'))

        elif element.get('type') is not None and element.get('type') == 'list':

            # Elements of the list
            children = [child for child in element
                        if child.tag == 'element']
            children_v = [int(child.get('index'))
                          for child in element if child.tag == 'element']
            # print(children_v)

            # Sorted
            idx = np.argsort(children_v)
            children = [children[i] for i in idx]

            # Append values
            for child in children:
                # print( child.get('index') )
                name = '{}_{}'.format(element.get('name'), child.get('index'))
                results[name] = float(child.get('value'))

        else:
            print('    Errror: result has no usable data ({})'.format(element))

    return results
