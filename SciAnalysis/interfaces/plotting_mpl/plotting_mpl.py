import matplotlib.pyplot as plt

# store results decorator for plotting library
# as of now function that returns decorator takes no arguments
def store_results(**options):
    ''' options are the plotting options
    '''
    def store_results_decorator(f):
        def f_new(*args, **kwargs):
            res = f(*args, **kwargs)
            store_results_plotting_mpl(res, **options)
            return res
        return f_new
    return store_results_decorator


def _cleanup_str(string):
    string = string.replace(" ", "_")
    string = string.replace("/", "_")
    string = string.replace("(", "_")
    string = string.replace(")", "_")
    string = string.replace(":", "_")
    return string

def store_results_plotting_mpl(results, **plot_opts):
    ''' Store the results to a numpy file.
        This saves to numpy format by default.
        May raise an error if it doesn't understand data.

        For images, you'll need to use a plotting/image interface (not implemented yet).
    '''
    if 'file_format' in plot_opts:
        file_format = plot_opts['file_format']
    else:
        file_format = "jpg"

    if 'plot_kws' in plot_opts:
        plot_kws = plot_kws

    if 'outputs' not in results:
        raise ValueError("attributes not in the sciresults. (Is this a valid SciResult object?)")
    data = results['outputs']

    if 'attributes' not in results:
        raise ValueError("attributes not in the sciresults. (Is this a valid SciResult object?)")
    attrs = results['attributes']

    if 'experiment_cycle' not in attrs:
        raise ValueError("Error cannot find experiment_cycle in attributes")
    if 'experiment_group' not in attrs:
        raise ValueError("Error cannot find experiment_group in attrbutess") 
    if 'sample_savename' not in attrs:
        raise ValueError("Error cannot find sample_savename in attributes")
    if 'protocol_name' not in attrs:
        raise ValueError("Error cannot find protocol_name in attributes")
    if 'scan_id' not in attrs:
        raise ValueError("Error cannot find scan_id in attributes")

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
    outdir = _ROOTDIR + "/" + experiment_cycle + "/" + experiment_group + "/" + protocol_name
    make_dir(outdir)
    outfile = outdir + "/" + sample_savename + "_" + scan_id
    outfile = outfile + "." + file_format

    fig = plt.figure()

    # now do the plotting

_plot_defaults =
{
    'label_size' : None,
    'xlabel' : None,
    'ylabel' : None,
    'show_xaxis' : None,

}

def parse_fig_options(fig, plot_opts):
    ax = fig.gca()
    if 'label_size' in plot_opts:
        label_size = plot_opts['label_size']
    else: 
        label_size = None

    if 'xlabel' in plot_opts:
        ax.set_xlabel(plot_opts['xlabel'], label_size=label_size)

class Plotter2D:
    def __init__(self, img=None, fignum=None, **kwargs):
        if fignum is None:
            self.fig = plt.figure()
        else:
            self.fig = plt.figure(fignum)

        self.ax = plt.gca()

        if img is not None:
            self.ax.imshow(img)
        # some nice plotting params to add later
        #rc('axes',linewidth=2)
        #label_size = 20
        #ticklabel_size=15
        #rcParams['xtick.labelsize'] = ticklabel_size
        #rcParams['ytick.labelsize'] = ticklabel_size

    def set_extent(self, extent):
        self.extent = extent
        self.ax.cla()
        self.ax.imshow(self.img, extent=self.extent)





