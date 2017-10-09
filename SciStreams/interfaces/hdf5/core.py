import h5py
from ...utils.file import _make_fname_from_attrs

# TODO : right now, overwrites file, should we check and maybe backup
# old file?

def _clean_attrs(attrs):
    ''' attrs is a dict. clean for hdf5 writing.
        This was mostly used to replace None with "None"
        for hdf5... But may eventually be useful for other stuff.
    '''
    for key, val in attrs.items():
        if isinstance(val, dict):
            _clean_attrs(val)
        elif val is None:
            attrs[key] = str(val)
        else:
            # do nothing
            pass

def store_results_hdf5(sdoc):
    # store a streamdoc to hdf5
    attrs = sdoc.get('attributes', {})
    outfile = _make_fname_from_attrs(attrs, filetype="hd5")

    f = h5py.File(outfile, 'w')

    # TODO : make a deep copy before modifying?
    _clean_attrs(attrs)
    add_element(f, 'attributes', attrs)

    # prepare args and kwargs into a dict
    kwargs = sdoc['kwargs']
    data = dict(kwargs)

    args = sdoc['args']
    for i, val in enumerate(args):
        key = "_arg{:03d}".format(i)
        data.update({key: val})

    add_element(f, 'data', data)

    f.close()


def add_element(h5group, name, data):
    if isinstance(data, dict):
        try:
            subgroup = h5group.create_group(name)
        except ValueError:
            subgroup = h5group[name]
        for key, val in data.items():
            add_element(subgroup, key, val)
    # NOTE : numpy arrays should not give True here
    elif isinstance(data, list):
        try:
            subgroup = h5group.create_group(name)
        except ValueError:
            subgroup = h5group[name]
        for i, val in enumerate(data):
            key = "item{:03d}".format(i)
            add_element(subgroup, key, val)
    else:
        h5group.create_dataset(name, data=data)
