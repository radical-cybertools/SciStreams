# doesn't need to be an object
import os.path


def make_dir(directory):
    ''' Creates directory if doesn't exist.'''
    if not os.path.isdir(directory):
        os.makedirs( directory )

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

# store results decorator
# first is a function that returns a decorator
# the decorator is a function that takes a function and 
# returns a new function
def store_results(outputs=None):
    def store_results_decorator(f):
        def f_new(*args, **kwargs):
            res = f(*args, **kwargs)
            store_results_xml(res, outputs=outputs)
            return res
        return f_new
    return store_results_decorator

def parse_attrs_xml(attrs):
    ''' parse the attributes into a string for each element'''
    import json
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
    if 'attributes' not in results:
        raise ValueError("attributes not in the sciresults. (Is this a valid SciResult object?)")

    attrs = results['attributes']

    if 'experiment_cycle' not in attrs or 'experiment_group' not in attrs or 'sample_savename' not in attrs or 'protocol_name' not in attrs:
        raise ValueError("Error cannot find experiment_cyle, experiment_group or sample_savename in results"
                "Either results object is not a sciresult or it has not been formatted properly."
                "Cannot save to XML")

    from SciAnalysis.config import XMLDIR
    from lxml import etree
    experiment_cycle = attrs['experiment_cycle']
    experiment_cycle = _cleanup_str(experiment_cycle)
    scan_id = str(attrs['scan_id'])
    scan_id = _cleanup_str(scan_id)
    experiment_group = attrs['experiment_group']
    experiment_group = _cleanup_str(experiment_group)
    sample_savename = attrs['sample_savename']
    sample_savename = _cleanup_str(sample_savename)
    protocol_name = attrs['protocol_name']
    protocol_name = _cleanup_str(protocol_name)
    outdir = XMLDIR + "/" + experiment_cycle + "/" + experiment_group +"/" + protocol_name
    make_dir(outdir)
    outfile = outdir + "/" + sample_savename + "_" + scan_id + ".xml"
    # just add to the sciresults so user knows it's been saved to xml
    results['attributes']['xml-outfile'] = outfile

    if os.path.isfile(outfile):
        # Result XML file already exists
        parser = etree.XMLParser(remove_blank_text=True)
        root = etree.parse(outfile, parser).getroot()

    else:
        # Create new XML file
        # TODO: Add characteristics of outfile
        root = etree.Element('DataFile', name=sample_savename)

    # TODO : instead of parsing (changing to str), walk through all elements in tree of dicts
    attrs_parsed = parse_attrs_xml(attrs)
    prot = etree.SubElement(root, 'protocol', **attrs_parsed)

    # Saving to xml
    if outputs is not None:
        for output in outputs:
            name = output
            content = results['outputs'][name]
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

def get_result(infile, protocol):
    '''Extracts a list of results for the given protocol, from the specified
    xml file. The most recent run of the protocol is used.'''
    # NOTE : Not tested yet
    import numpy as np
    from lxml import etree

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
