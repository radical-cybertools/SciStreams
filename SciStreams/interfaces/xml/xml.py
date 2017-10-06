# doesn't need to be an object
import os.path
import numpy as np
from lxml import etree

from ...utils.file import _make_fname_from_attrs

from ... import config
_ROOTDIR = config.resultsroot
_ROOTMAP = config.resultsrootmap


def store_results_xml(sdoc):
    '''
        expects a experiment_cycle, experiment group and sample_savename
        path is ROOT/experiment_cycle/experiment_group/sample_savename.xml

        The information for the filename to write to is also contained in the
        streamdoc provided.
    '''
    attrs = sdoc['attributes']
    outfile = _make_fname_from_attrs(attrs)

    # TODO should we append to existing xml file (if exists?)
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
            sample_savename = "no_sample_savename"
        root = etree.Element('DataFile', name=sample_savename)

    tree = sdoc_to_xml(sdoc, root)
    tree.write(outfile, pretty_print=True)


def sdoc_to_xml(sdoc, root):
    ''' make xml tree from a StreamDoc.
        Start from the root given in the xml tree.
    '''
    from lxml import etree

    attrs = sdoc['attributes']
    data_dict = sdoc['kwargs'].copy()
    args = sdoc['args']
    for i, arg in enumerate(args):
        data_dict.update({'_arg{:02d}'.format(i): arg})

    add_element(root, 'attributes', attrs)
    add_element(root, 'data', data_dict)

    return etree.ElementTree(root)


def add_element(root, name, data):
    from lxml import etree
    if isinstance(data, dict):
        subel = etree.SubElement(root, name)
        for k, v in data.items():
            add_element(subel, k, v)
    elif isinstance(data, list) or isinstance(data, np.ndarray):
        subel = etree.SubElement(root, name)
        for i, element in enumerate(data):
            k = 'element{:03d}'.format(i)
            add_element(subel, k, element)
    else:
        # make whatever it is into a string (safe)
        tag = {name: str(data)}
        etree.SubElement(root, 'element', **tag)


# TODO : write the get routine again
