import matplotlib
matplotlib.use("Agg")  # noqa
import numpy as np
import matplotlib.pyplot as plt

from ... import config

from uuid import uuid4

from ...tools.image import findLowHigh

from ...utils.file import _make_fname_from_attrs

from SciStreams.loggers import logger


_ROOTDIR = config.resultsroot
_ROOTMAP = config.resultsrootmap

global_list = set()
# usually the number of processes is good
maxfigs = 1000


class FigureGetter:
    maxfigs = 1000
    fignum = None

    def __enter__(self):
        found = False
        for i in range(self.maxfigs):
            if i in global_list:
                pass
            else:
                found = True
            if found:
                global_list.add(i)
                self.fignum = i
                break
        if found:
            fig = plt.figure(i)
            fig.clf()
            # forces new axes
            fig.gca()
            return fig
        else:
            raise Exception("Cannot lock a figure")

    def __exit__(self, exc_type, exc_val, exc_tb):
        plt.close(plt.figure(self.fignum))
        global_list.remove(self.fignum)


# store results decorator for plotting library
# as of now function that returns decorator takes no arguments


# TODO move to general tools

def store_results_mpl(data, attrs, **kwargs):
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
                images : keys of images
                lines : keys of lines to plot (on top of images)
                    if element is a tuple, assume (x,y) format, else assume
                    it's just y
                   elabelsize
                xlabel
                ylabel
                title
                all others are sent as plot options forwarded to matplotlib
    '''
    #data = sdoc['kwargs']
    #attrs = sdoc['attributes']
    img_norm = kwargs.pop('img_norm', None)
    # make import local so objects are not pickled
    # TODO : move some of the plotting into a general object
    try:
        outfile = _make_fname_from_attrs(attrs, filetype="png")
    except ValueError:
        # write to the same file for error
        print("Error, could not save file")
        # TODO maybe do a soft error
        raise

    print("writing to {}".format(outfile))

    images = kwargs.pop('images', [])
    lines = kwargs.pop('lines', [])
    linecuts = kwargs.pop('linecuts', [])

    # plotting the extra options
    labelsize = kwargs.pop('labelsize', 20)
    scale = kwargs.pop('scale', None)
    xlabel = kwargs.pop('xlabel', None)
    ylabel = kwargs.pop('ylabel', None)
    title = kwargs.pop('title', None)
    hideaxes = kwargs.pop('hideaxes', False)

    # now the rest should be plot keywords
    plot_kws = kwargs

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
    with FigureGetter() as fig:
        # import matplotlib.pyplot as plt
        # fig = plt.figure(fignum)  # int(np.random.random()*MAXFIGNUM))

        # only plot either of the three
        if len(images) > 0:
            logger.debug("store_results_mpl: got images")
            logger.debug("store_results_mpl: got images: {}".format(images))
            xlims, ylims, fig = plot_images(images, data, img_norm, plot_kws,
                                            fig=fig)
        elif len(lines) > 0:
            logger.debug("store_results_mpl: got lines")
            logger.debug("store_results_mpl: got lines: {}".format(lines))
            xlims, ylims, fig = plot_lines(lines, data, img_norm, plot_kws,
                                           xlims=xlims, ylims=ylims, fig=fig)
        elif len(linecuts) > 0:
            logger.debug("store_results_mpl: got linecuts: {}".format(linecuts))
            plot_linecuts(linecuts, data, img_norm, plot_kws,
                          xlims=xlims, ylims=ylims, fig=fig)

        if xlabel is not None:
            # for ax in fig.axes:
            # TODO : Allow labeling on multiple axes?
            ax = fig.axes[0]
            ax.set_xlabel(xlabel, size=labelsize)

        if ylabel is not None:
            # for ax in fig.axes:
            ax = fig.axes[0]
            ax.set_ylabel(ylabel, size=labelsize)

        if title is not None:
            for ax in fig.axes:
                ax.set_title(title)

        if scale is not None:
            try:
                if scale == 'loglog':
                    for ax in fig.axes:
                        ax.set_xscale('log')
                        ax.set_yscale('log')
                    correct_ylimits(ax)
                elif scale == 'semilogx':
                    for ax in fig.axes:
                        ax.set_xscale('log')
                elif scale == 'semilogy':
                    for ax in fig.axes:
                        ax.set_yscale('log')
                    correct_ylimits(ax)
                # else ignore
            except Exception:
                print("plotting_mpl : Error in setting " +
                      "scales (array is likely zeros)")

        if hideaxes:
            for ax in fig.axes:
                ax.get_xaxis().set_visible(False)
                ax.get_yaxis().set_visible(False)

        if xlims is not None:
            for ax in fig.axes:
                ax.set_xlim(*xlims)
        if ylims is not None:
            for ax in fig.axes:
                ax.set_ylim(*ylims)

        # save
        logger.debug("store_results_mpl : Trying to save to {}".format(outfile))
        try:
            fig.savefig(outfile)
        except Exception:
            logger.debug("Error in fig saving, ignoring... file : {}".format(outfile))
            raise
        # make sure no mem leaks, just close
        # figuregetter takes care of closing
        # plt.close(fig)


def plot_linecuts(linecuts_keys, data, img_norm, plot_kws, xlims=None,
                  ylims=None, fig=None):
    ''' assume that each linecut is a 2d image meant to be plotted as 1d
        linecuts can be tuples or one key. if tuple, first index assumed x-axis
    '''
    import matplotlib.pyplot as plt
    if fig is None:
        print("Warning, fignum is None, choosing random")
        fig = plt.figure(str(uuid4()))

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
                ax = fig.add_subplot(gs[i, :])
                # y should be 2d image
                if ylabels is not None:
                    tmplabel = "value : {}".format(ylabels[i])
                else:
                    tmplabel = "value : {}".format(i)
                ax.plot(x, linecut, label=tmplabel, **plot_kws)
                ax.legend()

    return xlims, ylims, fig


def plot_lines(lines, data, img_norm, plot_kws, xlims=None, ylims=None,
               fig=None):
    import matplotlib.pyplot as plt
    if fig is None:
        print("Warning, axes not passed. choosing random")
        fig = plt.figure(str(uuid4()))

    if len(fig.axes) == 0:
        # forces getting a new axis
        fig.gca()

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
            for ax in fig.axes:
                ax.plot(x, y, **new_opts)
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

    return xlims, ylims, fig


def plot_images(images, data, img_norm, plot_kws, xlims=None, ylims=None,
                fig=None):
    import matplotlib.pyplot as plt
    if fig is None:
        print("Warning, axes not passed. choosing random")
        fig = plt.figure(str(uuid4()))

    if len(fig.axes) == 0:
        # forces getting a new axis
        fig.gca()

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
                    im = fig.axes[0].imshow(image, **plot_kws)
                    fig.colorbar(im)
                    # TODO : verify this is clean with threads?
                    # plt.colorbar(cax=ax)
            elif image.ndim == 3:
                # make the fig again but with subplots
                nimgs = image.shape[0]
                dim = int(np.ceil(np.sqrt(nimgs)))
                # fig, axes = plt.subplots(dim, dim)
                # trying to make this thread safe
                gs = plt.GridSpec(dim, dim)
                gs.update(hspace=0.0, wspace=0.0)
                for j in range(len(image)):
                    if isinstance(image, np.ndarray):
                        ax = fig.add_subplot(gs[j//dim, j % dim])
                        ax.imshow(image[j], **plot_kws)
        else:
            print("Warning : key {} not found ".format(key) +
                  "in data for plotting(mpl)")

    return xlims, ylims, fig


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
