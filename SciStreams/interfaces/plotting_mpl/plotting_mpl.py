import matplotlib
matplotlib.use("Agg")  # noqa
from ... import config

from ...tools.image import findLowHigh

from ...utils.file import _make_fname_from_attrs

import numpy as np

from collections import deque

_ROOTDIR = config.resultsroot
_ROOTMAP = config.resultsrootmap

fig_buffer = deque()

# store results decorator for plotting library
# as of now function that returns decorator takes no arguments


# TODO move to general tools

def store_results_mpl(sdoc, **kwargs):
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
                    if element is a tuple, assume (x,y) format, else assume
                    it's just y
                   elabelsize
                xlabel
                ylabel
                title
    '''
    # if a failure don't even plot
    if sdoc['status'].lower() == 'failure':
        return
    data = sdoc['kwargs']
    attrs = sdoc['attributes']
    # NOTE : This is different from the interface version which expect a
    # StreamDoc as input
    img_norm = kwargs.get('img_norm', None)
    plot_kws = kwargs.get('plot_kws', {})
    # make import local so objects are not pickled
    import matplotlib.pyplot as plt
    # TODO : move some of the plotting into a general object
    try:
        outfile = _make_fname_from_attrs(attrs, filetype="png")
    except ValueError:
        # write to the same file for error
        print("Error, could not save file")
        # TODO maybe do a soft error
        raise

    print("writing to {}".format(outfile))

    images = kwargs.get('images', [])
    lines = kwargs.get('lines', [])
    linecuts = kwargs.get('linecuts', [])

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

    # only plot either of the three
    if len(images) > 0:
        xlims, ylims = plot_images(images, data, img_norm, plot_kws)
    elif len(lines) > 0:
        xlims, ylims = plot_lines(lines, data, img_norm,
                                  plot_kws, xlims=xlims, ylims=ylims)
    elif len(linecuts) > 0:
        plot_linecuts(linecuts, data, img_norm, plot_kws,
                      xlims=xlims, ylims=ylims)

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

    if xlims is not None:
        plt.xlim(*xlims)
    if ylims is not None:
        plt.ylim(*ylims)

    # save
    try:
        fig.savefig(outfile)
    except Exception:
        print("Error in fig saving, ignoring... file : {}".format(outfile))
    # make sure no mem leaks, just close
    # plt.close(fig)


def plot_linecuts(linecuts_keys, data, img_norm, plot_kws, xlims=None,
                  ylims=None):
    ''' assume that each linecut is a 2d image meant to be plotted as 1d
        linecuts can be tuples or one key. if tuple, first index assumed x-axis
    '''
    import matplotlib.pyplot as plt
    # assumes plot has been cleared already
    # and fig selected
    for linecuts_key in linecuts_keys:
        if isinstance(linecuts_key, tuple) and len(linecuts_key) > 1:
            if linecuts_key[0] in data and linecuts_key[1] in data:
                x = data[linecuts_key[0]]
                y = data[linecuts_key[1]]
                if len(linecuts_key) > 2:
                    ylabels = data[linecuts_key[2]]
                else:
                    ylabels = None
            else:
                x, y, ylabels = None, None
        else:
            if linecuts_key in data:
                y = data[linecuts_key]
                x = np.arange(len(y))
                ylabels = None
            else:
                x, y, ylabels = None, None

        if xlims is None:
            xlims = [np.nanmin(x), np.nanmax(x)]
        else:
            xlims[0] = np.nanmin([np.nanmin(x), xlims[0]])
            xlims[1] = np.nanmax([np.nanmax(x), xlims[1]])

        # assume y is an array of arrays...
        gs = plt.GridSpec(len(y), 1)
        gs.update(hspace=0.0, wspace=0.0)
        for i, linecut in enumerate(y):
            # only plot if there is data
            if x is not None and linecut is not None:
                ax = plt.subplot(gs[i, :])
                plt.sca(ax)
                # y should be 2d image
                if ylabels is not None:
                    tmplabel = "value : {}".format(ylabels[i])
                else:
                    tmplabel = "value : {}".format(i)
                plt.plot(x, linecut, label=tmplabel, **plot_kws)
                plt.legend()

    return xlims, ylims


def plot_lines(lines, data, img_norm, plot_kws, xlims=None, ylims=None):
    import matplotlib.pyplot as plt
    for line in lines:
        # reset the per data plot opts (only set if line is a dict)
        opts = {}
        if isinstance(line, tuple) and len(line) == 2:
            if line[0] in data and line[1] in data:
                x = data[line[0]]
                y = data[line[1]]
            else:
                x, y = None, None
        elif isinstance(line, dict):
            # make a copy of line
            line = dict(line)
            xkey = line.pop('x', None)
            ykey = line.pop('y', None)

            if ykey is not None:
                if ykey not in data:
                    errormsg = "Error {} y key not in data.".format(ykey)
                    errormsg += "\n Data keys : {}".format(list(data.keys()))
                    raise ValueError(errormsg)
                y = data[ykey]
            else:
                y = None

            if xkey is not None:
                if xkey not in data:
                    errormsg = "Error {} x key not in data.".format(xkey)
                    errormsg += "\n Data keys : {}".format(list(data.keys()))
                    raise ValueError(errormsg)
                x = data[xkey]
            else:
                x = None

            if x is None and y is not None:
                x = np.arange(len(y))

            opts = line
        else:
            if line in data:
                y = data[line]
                x = np.arange(len(y))
            else:
                x, y = None, None

        if x is not None and y is not None:
            new_opts = opts.copy()
            new_opts.update(plot_kws)
            plt.plot(x, y, **new_opts)
            if xlims is None:
                xlims = [np.nanmin(x), np.nanmax(x)]
            else:
                xlims[0] = np.nanmin([np.nanmin(x), xlims[0]])
                xlims[1] = np.nanmax([np.nanmax(x), xlims[1]])
            if ylims is None:
                ylims = [np.nanmin(y), np.nanmax(y)]
            else:
                ylims[0] = np.nanmin([np.nanmin(y), ylims[0]])
                ylims[1] = np.nanmax([np.nanmax(y), ylims[1]])

    return xlims, ylims


def plot_images(images, data, img_norm, plot_kws, xlims=None, ylims=None):
    import matplotlib.pyplot as plt
    # print("plotting images with keys {}".format(images))
    for key in images:
        # print("found image with key {}".format(key))
        # find some reasonable color scale
        if key in data:
            image = data[key]
            if img_norm is not None:
                # print("normalizing image")
                image = img_norm(image)
            vmin, vmax = findLowHigh(image)
            # print("image :{}".format(image))
            # print("vmin : {}, vmax: {}".format(vmin, vmax))
            if 'vmin' not in plot_kws:
                plot_kws['vmin'] = vmin
            if 'vmax' not in plot_kws:
                plot_kws['vmax'] = vmax
            if image.ndim == 2:
                if isinstance(image, np.ndarray):
                    plt.imshow(image, **plot_kws)
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

    return xlims, ylims


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
