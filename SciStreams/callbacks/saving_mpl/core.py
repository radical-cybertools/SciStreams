import matplotlib
matplotlib.use("Agg")  # noqa
from ... import config
import os.path

from ...tools.image import findLowHigh

from bluesky.callbacks import CallbackBase

import numpy as np

from collections import deque

_ROOTDIR = config.resultsroot
_ROOTMAP = config.resultsrootmap

fig_buffer = deque()

# store results decorator for plotting library
# as of now function that returns decorator takes no arguments


# TODO move to general tools
def make_dir(directory):
    ''' Creates directory if doesn't exist.'''
    if not os.path.isdir(directory):
        os.makedirs(directory)


def _cleanup_str(string):
    string = string.replace(" ", "_")
    string = string.replace("/", "_")
    string = string.replace("(", "_")
    string = string.replace(")", "_")
    string = string.replace(":", "_")
    return string


def _make_fname_from_attrs(**attrs):
    ''' make filename from attributes.
        This will likely be copied among a few interfaces.
    '''

    # remove the trailing slash, and index if a
    # list of strs
    rootdir = attrs['experiment_alias_directory']
    if not isinstance(rootdir, str):
        rootdir = rootdir[0]
    rootdir = rootdir.strip("/")

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

    outdir = rootdir + "/" + detector_name + "/" + stream_name + "/plots"
    make_dir(outdir)
    outfile = outdir + "/" + sample_savename + "_" + scan_id

    return outfile

from .. import CallbackBase
class StorePlot_MPL(CallbackBase):
    def __init__(self, **kwargs):
        ''' kwargs are the options for store_results.
            see store_results for the kwargs details
        '''
        self.kwargs = kwargs
        self.start_uid = None
        self.descriptor_uid = None
        # don't pass the kwargs or args to CallbackBase
        super(StorePlot_MPL, self).__init__()

    def start(self, doc):
        # for each new start save the metadata
        #print("Got start: {}".format(doc))
        self.md = dict()
        self.start_uid = doc['uid']
        for key, val in doc.items():
            self.md[key] = val

    def descriptor(self, doc):
        #print("Got descriptor: {}".format(doc))
        if self.start_uid != doc['run_start']:
            errormsg = "Error, uid of descriptor and run start"
            errormsg += " do not match. Perhaps a run start"
            errormsg += " and descriptor are out of sync"
            raise ValueError(errormsg)

        self.descriptor_uid = doc['uid']

    # approach for now will be to assume all necessary data is in the event
    # itself
    def event(self, doc):
        #print("Got event: {}".format(doc))
        data = doc['data']
        if doc['descriptor'] != self.descriptor_uid:
            errormsg = "Error, uid of event and run start"
            errormsg += " do not match. Perhaps a run start"
            errormsg += " and descriptor are out of sync"
            raise ValueError(errormsg)
        #attrs = self.kwargs
        attrs = self.md
        # use the store_results function in this file
        # (i.e. don't include it in object)
        store_results(data, attrs, **self.kwargs)

    def stop(self, doc):
        # clear state
        self.start_uid = None
        self.descriptor_uid = None


def store_results(data, attrs, **kwargs):
    ''' Store the results to a numpy file.
        This saves to numpy format by default.
        May raise an error if it doesn't understand data.
        Expects a StreamDoc

        data : the data itself (a dict)
        attrs : the attributes of the data
            This is used to create the kwargs

        For images, you'll need to use a plotting/image interface (not
        implemented yet).

        kwargs : options as follows:
            keywords:
                plot_kws : plot options forwarded to matplotlib
                images : keys of images
                lines : keys of lines to plot (on top of images)
                    if element is a tuple, assume (x,y) format, else assume it's
                    just y
                   elabelsize
                xlabel
                ylabel
                title
    '''
    # NOTE : This is different from the interface version which expect a
    # StreamDoc as input
    img_norm = kwargs.get('img_norm', None)
    plot_kws = kwargs.get('plot_kws', {})
    import matplotlib.pyplot as plt
    # TODO : move some of the plotting into a general object

    outfile = _make_fname_from_attrs(**attrs) + ".png"
    print("writing to {}".format(outfile))

    images = kwargs.get('images', [])
    lines = kwargs.get('lines', [])
    linecuts = kwargs.get('linecuts', [])

    stream_name = attrs.get("stream_name", "noname")

    xlims = None
    ylims = None
    # plt.ioff()
    '''
    timeout = 100# in seconds
    t_start = time.time()
    while(len(fig_queue) == 0):
        if time.time() - t_start > timeout:
            raise Exception("Timed out when waiting for a figure to be
                released")
        time.sleep(1)
    '''

    # grab fig from queue. do some cleanup (if a fig
    # has not been released for some time, release it)
    fig = plt.figure(0)  # int(np.random.random()*MAXFIGNUM))
    fig.clf()
    ax = fig.gca()

    plot_images(images, data, img_norm, plot_kws)
    xlims, ylims = plot_lines(lines, data, img_norm,
            plot_kws,xlims=xlims,ylims=ylims)
    xlims, ylims = plot_linecuts(linecuts, data, img_norm, plot_kws,
            xlims=xlims, ylims=ylims)


    if xlims is not None:
        plt.xlim(xlims[0], xlims[1])
    if ylims is not None:
        plt.ylim(ylims[0], ylims[1])

    # plotting the extra options
    if 'labelsize' in plot_kws:
        labelsize = plot_kws['labelsize']
    else:
        labelsize = 20

    if 'hideaxes' in plot_kws:
        hideaxes = plot_kws['hideaxes']
    else:
        hideaxes = False

    if 'xlabel' in plot_kws:
        xlabel = plot_kws['xlabel']
        plt.xlabel(xlabel, size=labelsize)

    if 'ylabel' in plot_kws:
        ylabel = plot_kws['ylabel']
        plt.xlabel(xlabel, size=labelsize)
        plt.ylabel(ylabel, size=labelsize)

    if 'title' in plot_kws:
        title = plot_kws['title']
        plt.title(title)

    if 'scale' in plot_kws:
        try:
            scale = plot_kws['scale']
            if scale == 'loglog':
                ax.set_xscale('log')
                ax.set_yscale('log')
                correct_ylimits(ax)
            elif scale == 'semilogx':
                ax.set_xscale('log')
            elif scale == 'semilogy':
                ax.set_yscale('log')
                correct_ylimits(ax)
            # else ignore
        except Exception:
            print("plotting_mpl : Error in setting " +
                  "scales (array is likely zeros)")

    if hideaxes:
        ax.get_xaxis().set_visible(False)
        ax.get_yaxis().set_visible(False)

    # save
    try:
        fig.savefig(outfile)
    except Exception:
        print("Error in fig saving, ignoring... file : {}".format(outfile))
    # make sure no mem leaks, just close
    plt.close(fig)


def plot_linecuts(linecuts, data, img_norm, plot_kws, xlims=None, ylims=None):
    ''' assume that each linecut is a 2d image meant to be plotted as 1d
        linecuts can be tuples or one key. if tuple, first index assumed x-axis
    '''
    import matplotlib.pyplot as plt
    for linecut in linecuts:
        if isinstance(linecut, tuple) and len(linecut) == 2:
            if linecut[0] in data and linecut[1] in data:
                x = data[linecut[0]]
                y = data[linecut[1]]
            else:
                x, y = None, None
        else:
            if line in data:
                y = data[line]
                x = np.arange(len(y))
            else:
                x, y = None, None
        if x is not None and y is not None:
            for suby in y:
                # y should be 2d image
                plt.plot(x, suby, **plot_kws)
                if xlims is None:
                    xlims = [np.min(x), np.max(x)]
                else:
                    xlims[0] = np.min([np.min(x), xlims[0]])
                    xlims[1] = np.max([np.max(x), xlims[1]])

            # dont set ylim from for loop
            if ylims is None:
                ylims = [np.min(y), np.max(y)]
            else:
                ylims[0] = np.min([np.min(y), ylims[0]])
                ylims[1] = np.max([np.max(y), ylims[1]])

    return xlims, ylims

def plot_lines(lines, data, img_norm, plot_kws, xlims=None, ylims=None):
    import matplotlib.pyplot as plt
    for line in lines:
        if isinstance(line, tuple) and len(line) == 2:
            if line[0] in data and line[1] in data:
                x = data[line[0]]
                y = data[line[1]]
            else:
                x, y = None, None
        else:
            if line in data:
                y = data[line]
                x = np.arange(len(y))
            else:
                x, y = None, None
        if x is not None and y is not None:
            plt.plot(x, y, **plot_kws)
            if xlims is None:
                xlims = [np.min(x), np.max(x)]
            else:
                xlims[0] = np.min([np.min(x), xlims[0]])
                xlims[1] = np.max([np.max(x), xlims[1]])
            if ylims is None:
                ylims = [np.min(y), np.max(y)]
            else:
                ylims[0] = np.min([np.min(y), ylims[0]])
                ylims[1] = np.max([np.max(y), ylims[1]])

    return xlims, ylims

def plot_images(images, data, img_norm, plot_kws):
    import matplotlib.pyplot as plt
    for key in images:
        # find some reasonable color scale
        if key in data:
            image = data[key]
            if img_norm is not None:
                print("normalizing image")
                image = img_norm(image)
            vmin, vmax = findLowHigh(image)
            if 'vmin' in plot_kws:
                vmin = plot_kws['vmin']
            if 'vmax' in plot_kws:
                vmax = plot_kws['vmax']
            if image.ndim == 2:
                if isinstance(image, np.ndarray):
                    plt.imshow(image, vmin=vmin, vmax=vmax, **plot_kws)
                    plt.colorbar()
            elif image.ndim == 3:
                nimgs = image.shape[0]
                dim = int(np.ceil(np.sqrt(nimgs)))
                fig, axes = plt.subplots(dim, dim)
                axes = np.array(axes).ravel()
                for j in range(len(image)):
                    if isinstance(image, np.ndarray):
                        axes[j].imshow(image[j], **plot_kws)
        else:
            print("Warning : key {} not found ".format(key) +
                  "in data for plotting(mpl)")

def correct_ylimits(ax):
    # correct for funky plots, mainly for loglog
    lns = ax.get_lines()
    vmin = None
    for ln in lns:
        x, y = ln.get_data()
        w = np.where(y > 0)
        vmintmp = np.min(y[w])
        if vmin is None:
            vmin = vmintmp
        else:
            vmin = np.minimum(vmin, vmintmp)
    ax.set_ylim(vmin, None)
