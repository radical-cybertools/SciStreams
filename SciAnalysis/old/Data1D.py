#!/usr/bin/python
# -*- coding: utf-8 -*-
# vi: ts=4 sw=4
'''
:mod:`SciAnalysis.Data` - Base data objects for SciAnalysis
================================================
.. module:: SciAnalysis.Data
   :synopsis: Provides base classes for handling data
.. moduleauthor:: Dr. Kevin G. Yager <kyager@bnl.gov>
                    Brookhaven National Laboratory
'''

################################################################################
#  This code defines some baseline objects for handling data.
################################################################################
# Known Bugs:
#  N/A
################################################################################
# TODO:
#  Search for "TODO" below.
################################################################################


#import sys
import numpy as np
import pylab as plt
import matplotlib as mpl
from scipy import signal # For gaussian smoothing
from scipy import ndimage # For resize, etc.
from scipy import stats # For skew
#from scipy.optimize import leastsq
#import scipy.special

import PIL # Python Image Library (for opening PNG, etc.)

from . import tools





# DataLine
################################################################################
class DataLine(object):

    def __init__(self, infile=None, x=None, y=None, name=None, plot_args=None, **kwargs):

        if infile is None:
            self.x = x
            self.y = y
        else:
            self.load(infile, **kwargs)


        self.x_label = kwargs['x_label'] if 'x_label' in kwargs else 'x'
        self.y_label = kwargs['y_label'] if 'y_label' in kwargs else 'y'

        self.x_rlabel = kwargs['x_rlabel'] if 'x_rlabel' in kwargs else self.x_label
        self.y_rlabel = kwargs['y_rlabel'] if 'y_rlabel' in kwargs else self.y_label

        self.x_err = kwargs['x_err'] if 'x_err' in kwargs else None
        self.y_err = kwargs['y_err'] if 'y_err' in kwargs else None

        if name is not None:
            self.name = name
        elif infile is not None:
            self.name = tools.Filename(infile).get_filebase()
        else:
            self.name = None


        self.plot_valid_keys = ['color', 'linestyle', 'linewidth', 'marker', 'markerfacecolor', 'markersize', 'alpha', 'markeredgewidth', 'markeredgecolor']
        self.plot_args = { 'color' : 'k',
                        'marker' : 'o',
                        'linewidth' : 3.0,
                        'rcParams': {'axes.labelsize': 35,
                                        'xtick.labelsize': 30,
                                        'ytick.labelsize': 30,
                                        },
                            }
        if plot_args: self.plot_args.update(plot_args)


    # Data loading
    ########################################

    def load(self, infile, format='auto', **kwargs):
        '''Loads data from the specified file.'''

        f = tools.Filename(infile)
        ext = f.get_ext()[1:]

        if format=='custom':
            x, y = self.load_custom(infile, **kwargs)
            self.x = x
            self.y = y

        elif format=='npy' or ext=='npy':
            data = np.load(infile)
            self.x = data[:,0]
            self.y = data[:,1]

        elif format in ['auto'] or ext in ['dat', 'txt']:
            data = np.loadtxt(infile)
            self.x = data[:,0]
            self.y = data[:,1]

        else:
            print("Couldn't identify data format for %s."%(infile))


    def load_custom(self, infile, **kwargs):

        xvals = []
        yvals = []
        with open(infile) as fin:

            for i, line in enumerate(fin.readlines()):
                els = line.split()
                if i>=kwargs['skiplines'] and len(els)>1 and els[0][0]!=kwargs['comment_char']:
                    xvals.append( float(els[kwargs['xindex']]) )
                    yvals.append( float(els[kwargs['yindex']]) )

        x = np.asarray(xvals)
        y = np.asarray(yvals)

        return x, y


    def copy_labels(self, line):
        '''Copies labels (x, y) from the supplied line into this line.'''

        self.x_label = line.x_label
        self.y_label = line.y_label
        self.x_rlabel = line.x_rlabel
        self.y_rlabel = line.y_rlabel


    # Data export
    ########################################

    def save_data(self, outfile):

        if self.x_err is None and self.y_err is None:
            data = np.dstack([self.x, self.y])[0]
            header = '%s %s' % (self.x_label, self.y_label)

        elif self.y_err is None:
            data = np.dstack([self.x, self.x_err, self.y])[0]
            header = '%s %serr %s' % (self.x_label, self.x_label, self.y_label)

        elif self.x_err is None:
            data = np.dstack([self.x, self.y, self.y_err])[0]
            header = '%s %s %serr' % (self.x_label, self.y_label, self.y_label)

        else:
            data = np.dstack([self.x, self.x_err, self.y, self.y_err])[0]
            header = '%s %serr %s %serr' % (self.x_label, self.x_label, self.y_label, self.y_label)

        np.savetxt( outfile, data, header=header )


    # Data access
    ########################################
    def get_x_spacing(self, mode='avg'):
        '''Returns the x-spacing for the data.'''

        data = self.x

        num_x = len(data)
        xi = data[0]
        xf = data[-1]

        if mode=='avg':
            dx = (xf-xi)/(num_x-1)

        elif mode=='first':
            x2 = data[1]
            dx = (x2-xi)

        elif mode=='last':
            x2 = data[-2]
            dx = (xf-x2)

        elif mode=='middle':
            i_middle = int(1.0*num_x/2.0)
            x1 = data[i_middle]
            x2 = data[i_middle+1]
            dx = (x2-x1)

        else:
            print( 'Error in get_x_spacing' )

        return dx


    def sub_range(self, xi, xf):
        '''Returns a DataLine that only has a subset of the original x range.'''

        line = self.copy()
        line.trim(xi, xf)

        return line


    def target_x(self, target):
        '''Find the datapoint closest to the given x.'''

        self.sort_x()

        # Search through x for the target
        idx = np.where( self.x>=target )[0][0]
        xcur = self.x[idx]
        ycur = self.y[idx]

        return xcur, ycur


    def target_y(self, target):
        '''Find the datapoint closest to the given y.'''

        x = np.asarray(self.x)
        y = np.asarray(self.y)

        # Sort
        indices = np.argsort(y)
        x_sorted = x[indices]
        y_sorted = y[indices]

        # Search through x for the target
        idx = np.where( y_sorted>=target )[0][0]
        xcur = x_sorted[idx]
        ycur = y_sorted[idx]

        return xcur, ycur


    # Data modification
    ########################################
    def sort_x(self):
        '''Arrange (x,y) datapoints so that x is increasing.'''
        x = np.asarray(self.x)
        y = np.asarray(self.y)

        # Sort
        indices = np.argsort(x)
        self.x = x[indices]
        self.y = y[indices]

    def sort_y(self):
        x = np.asarray(self.x)
        y = np.asarray(self.y)

        # Sort
        indices = np.argsort(y)
        self.x = x[indices]
        self.y = y[indices]


    def trim(self, xi, xf):
        '''Reduces the data by trimming the x range.'''

        x = np.asarray(self.x)
        y = np.asarray(self.y)

        # Sort
        indices = np.argsort(x)
        x_sorted = x[indices]
        y_sorted = y[indices]

        if xi==None:
            idx_start = 0
        else:
            try:
                idx_start = np.where( x_sorted>xi )[0][0]
            except IndexError:
                idx_start = 0

        if xf==None:
            idx_end = len(x_sorted)
        else:
            try:
                idx_end = np.where( x_sorted>xf )[0][0]
            except IndexError:
                idx_end = len(x_sorted)

        self.x = x_sorted[idx_start:idx_end]
        self.y = y_sorted[idx_start:idx_end]


    def kill_x(self, x_center, x_spread):
        '''Removes some points from the line (within the specified range).'''

        x = np.asarray(self.x)
        y = np.asarray(self.y)

        # Sort
        indices = np.argsort(x)
        x_sorted = x[indices]
        y_sorted = y[indices]

        idx = np.where( abs(x_sorted-x_center)<x_spread )
        self.x = np.delete( x_sorted, idx )
        self.y = np.delete( y_sorted, idx )


    def remove_spurious(self, bins=5, tol=1e5):
        '''Remove data-points that deviate strongly from the curve.
        They are replaced with the local average.'''

        s = int(bins/2)
        for i, y in enumerate(self.y):

            sub_range = self.y[i-s:i+s]

            # average excluding point i
            avg = ( np.sum(self.y[i-s:i+s]) - y )/( len(sub_range) - 1 )

            if abs(y-avg)/avg>tol:
                self.y[i] = avg


    def smooth(self, sigma):

        self.y = ndimage.filters.gaussian_filter( self.y, sigma )




    # Data analysis
    ########################################
    def stats(self, prepend='stats_'):

        results = {}

        results[prepend+'max'] = np.max(self.y)
        results[prepend+'min'] = np.min(self.y)
        results[prepend+'average'] = np.average(self.y)
        results[prepend+'std'] = np.std(self.y)
        results[prepend+'N'] = len(self.y)
        results[prepend+'total'] = np.sum(self.y)

        results[prepend+'skew'] = stats.skew(self.y)

        results[prepend+'spread'] = results[prepend+'max'] - results[prepend+'min']
        results[prepend+'std_rel'] = results[prepend+'std'] / results[prepend+'average']

        zero_crossings = np.where(np.diff(np.signbit(self.y)))[0]
        results[prepend+'zero_crossings'] = len(zero_crossings)

        return results


    # Plotting
    ########################################

    def plot(self, save=None, show=False, plot_range=[None,None,None,None], plot_buffers=[0.2,0.05,0.2,0.05], **kwargs):
        '''Plots the scattering data.

        Parameters
        ----------
        save : str
            Set to 'None' to avoid saving to disk. Provide filename to save.
        show : bool
            Set to true to open an interactive window.
        plot_range : [float, float, float, float]
            Set the range of the plotting (None scales automatically instead).
        '''

        self._plot(save=save, show=show, plot_range=plot_range, plot_buffers=plot_buffers, **kwargs)


    def _plot(self, save=None, show=False, plot_range=[None,None,None,None], plot_buffers=[0.2,0.05,0.2,0.05], error=False, error_band=False, xlog=False, ylog=False, xticks=None, yticks=None, dashes=None, **kwargs):

        plot_args = self.plot_args.copy()
        plot_args.update(kwargs)
        self.process_plot_args(**plot_args)

        self.fig = plt.figure( figsize=(10,7), facecolor='white' )
        left_buf, right_buf, bottom_buf, top_buf = plot_buffers
        fig_width = 1.0-right_buf-left_buf
        fig_height = 1.0-top_buf-bottom_buf
        self.ax = self.fig.add_axes( [left_buf, bottom_buf, fig_width, fig_height] )




        p_args = dict([(i, plot_args[i]) for i in self.plot_valid_keys if i in plot_args])
        self._plot_main(error=error, error_band=error_band, dashes=dashes, **p_args)


        plt.xlabel(self.x_rlabel)
        plt.ylabel(self.y_rlabel)

        if xlog:
            plt.semilogx()
        if ylog:
            plt.semilogy()
        if xticks is not None:
            self.ax.set_xticks(xticks)
        if yticks is not None:
            self.ax.set_yticks(yticks)


        # Axis scaling
        xi, xf, yi, yf = self.ax.axis()
        if plot_range[0] != None: xi = plot_range[0]
        if plot_range[1] != None: xf = plot_range[1]
        if plot_range[2] != None: yi = plot_range[2]
        if plot_range[3] != None: yf = plot_range[3]
        self.ax.axis( [xi, xf, yi, yf] )

        self._plot_extra(**plot_args)

        if save:
            if 'dpi' in plot_args:
                plt.savefig(save, dpi=plot_args['dpi'])
            else:
                plt.savefig(save)

        if show:
            self._plot_interact()
            plt.show()

        plt.close(self.fig.number)


    def _plot_main(self, error=False, error_band=False, dashes=None, **plot_args):

        if error_band:
            # TODO: Make this work
            l, = plt.plot(self.x, self.y, **plot_args)
            self.ax.fill_between(self.x, self.y-self.y_err, self.y+self.y_err, facecolor='0.8', linewidth=0)

        elif error:
            l = plt.errorbar( self.x, self.y, xerr=self.x_err, yerr=self.y_err, **plot_args)

        else:
            #l, = plt.plot(self.x, self.y, **plot_args)
            l, = self.ax.plot(self.x, self.y, **plot_args)


        if dashes is not None:
            l.set_dashes(dashes)


    def _plot_extra(self, **plot_args):
        '''This internal function can be over-ridden in order to force additional
        plotting behavior.'''

        pass



    def process_plot_args(self, **plot_args):

        if 'rcParams' in plot_args:
            for param, value in plot_args['rcParams'].items():
                plt.rcParams[param] = value



    # Plot interaction
    ########################################

    def _plot_interact(self):

        self.fig.canvas.set_window_title('SciAnalysis')
        #plt.get_current_fig_manager().toolbar.pan()
        self.fig.canvas.toolbar.pan()
        self.fig.canvas.mpl_connect('scroll_event', self._scroll_event )
        #self.fig.canvas.mpl_connect('motion_notify_event', self._move_event )
        #self.fig.canvas.mpl_connect('key_press_event', self._key_press_event)

        #self.ax.format_coord = self._format_coord


    def _scroll_event(self, event):
        '''Gets called when the mousewheel/scroll-wheel is used. This activates
        zooming.'''

        if event.inaxes!=self.ax:
            return


        current_plot_limits = self.ax.axis()
        x = event.xdata
        y = event.ydata


        # The following function converts from the wheel-mouse steps
        # into a zoom-percentage. Using a base of 4 and a divisor of 2
        # means that each wheel-click is a 50% zoom. However, the speed
        # of zooming can be altered by changing these numbers.

        # 50% zoom:
        step_percent = 4.0**( -event.step/2.0 )
        # Fast zoom:
        #step_percent = 100.0**( -event.step/2.0 )
        # Slow zoom:
        #step_percent = 2.0**( -event.step/4.0 )

        xi = x - step_percent*(x-current_plot_limits[0])
        xf = x + step_percent*(current_plot_limits[1]-x)
        yi = y - step_percent*(y-current_plot_limits[2])
        yf = y + step_percent*(current_plot_limits[3]-y)

        self.ax.axis( (xi, xf, yi, yf) )

        self.fig.canvas.draw()


    # Object
    ########################################
    def copy(self):
        import copy
        return copy.deepcopy(self)


    # End class DataLine(object)
    ########################################



# DataLineAngle
################################################################################
class DataLineAngle (DataLine):

    def __init__(self, infile=None, x=None, y=None, name=None, plot_args=None, **kwargs):

        self.x = x
        self.y = y


        self.x_label = kwargs['x_label'] if 'x_label' in kwargs else 'angle (degrees)'
        self.y_label = kwargs['y_label'] if 'y_label' in kwargs else 'y'

        self.x_rlabel = kwargs['x_rlabel'] if 'x_rlabel' in kwargs else '$\chi \, (^{\circ})$'
        self.y_rlabel = kwargs['y_rlabel'] if 'y_rlabel' in kwargs else '$I(\chi)$'

        self.x_err = kwargs['x_err'] if 'x_err' in kwargs else None
        self.y_err = kwargs['y_err'] if 'y_err' in kwargs else None

        if name is not None:
            self.name = name
        elif infile is not None:
            self.name = tools.Filename(infile).get_filebase()
        else:
            self.name = None

        self.plot_valid_keys = ['color', 'linestyle', 'linewidth', 'marker', 'markerfacecolor', 'markersize', 'alpha']
        self.plot_args = { 'color' : 'k',
                        'marker' : 'o',
                        'linewidth' : 3.0,
                        'rcParams': {'axes.labelsize': 35,
                                        'xtick.labelsize': 30,
                                        'ytick.labelsize': 30,
                                        },
                            }
        if plot_args: self.plot_args.update(plot_args)



    # Data analysis
    ########################################
    def orientation_order_parameter(self, prepend='orientation_'):

        results = {}

        # TODO: Implement this

        results[prepend+'S'] = 0.0

        return results



    # Data modification
    ########################################

    def renormalize_symmetry(self, symmetry=2, verbosity=3):
        '''Divides the curve by the given symmetry (using an eta-function).
        This can be used to remove a known symmetry, thereby highlighting other
        features of the curve.'''


        lm_result, fit_line, fit_line_extended = self.fit_eta(self, symmetry=symmetry, verbosity=verbosity)
        self.y /= fit_line.y

        # Diagnostics
        #self.y = fit_line.y
        #for k, v in lm_result.params.items():
            #print(k, v)


    def fit_eta(self, line, **run_args):

        import lmfit

        def model(v, x):
            '''Eta orientation function.'''

            x = np.radians(x)

            m = v['prefactor']*( 1 - (v['eta']**2) )/( ((1+v['eta'])**2) - 4*v['eta']*( np.square(np.cos(  (v['symmetry']/2.0)*(x-v['x_center'])  )) ) ) + v['baseline']
            return m

        def func2minimize(params, x, data):

            v = params.valuesdict()
            m = model(v, x)

            return m - data

        params = lmfit.Parameters()
        params.add('prefactor', value=np.max(line.y), min=0)
        params.add('x_center', value=np.average(line.x), min=np.min(line.x), max=np.max(line.x))
        params.add('eta', value=0.4, min=0, max=1, vary=True)
        params.add('symmetry', value=run_args['symmetry'], vary=False)
        #params.add('baseline', value=np.min(line.y), vary=False)
        params.add('baseline', value=0, vary=False)


        lm_result = lmfit.minimize(func2minimize, params, args=(line.x, line.y))

        if run_args['verbosity']>=5:
            print('Fit results (lmfit):')
            lmfit.report_fit(lm_result.params)


        fit_x = line.x
        fit_y = model(lm_result.params.valuesdict(), fit_x)
        fit_line = DataLine(x=fit_x, y=fit_y, plot_args={'linestyle':'-', 'color':'r', 'marker':None, 'linewidth':4.0})

        fit_x = np.linspace(np.min(line.x), np.max(line.x), num=200)
        fit_y = model(lm_result.params.valuesdict(), fit_x)
        fit_line_extended = DataLine(x=fit_x, y=fit_y, plot_args={'linestyle':'-', 'color':'r', 'marker':None, 'linewidth':4.0})

        return lm_result, fit_line, fit_line_extended



    # Plotting
    ########################################

    def plot_polar(self, save=None, show=False, size=5, plot_buffers=[0.1,0.1,0.1,0.1], **kwargs):
        '''Plots the scattering data.

        Parameters
        ----------
        save : str
            Set to 'None' to avoid saving to disk. Provide filename to save.
        show : bool
            Set to true to open an interactive window.
        plot_range : [float, float, float, float]
            Set the range of the plotting (None scales automatically instead).
        '''

        self._plot_polar(save=save, show=show, size=size, plot_buffers=plot_buffers, **kwargs)


    def _plot_polar(self, save=None, show=False, size=5, plot_buffers=[0.2,0.2,0.2,0.2], assumed_symmetry=2, **kwargs):

        # TODO: Recast as part of plot_args
        #plt.rcParams['font.family'] = 'sans-serif'
        plt.rcParams['axes.labelsize'] = 20
        plt.rcParams['xtick.labelsize'] = 15
        plt.rcParams['ytick.labelsize'] = 15


        self.fig = plt.figure( figsize=(size,size), facecolor='white' )
        left_buf, right_buf, bottom_buf, top_buf = plot_buffers
        fig_width = 1.0-right_buf-left_buf
        fig_height = 1.0-top_buf-bottom_buf
        self.ax = self.fig.add_axes( [left_buf, bottom_buf, fig_width, fig_height], polar=True )
        self.ax.set_theta_direction(-1)
        self.ax.set_theta_zero_location('N')

        plot_args = self.plot_args.copy()
        plot_args.update(kwargs)



        p_args = dict([(i, plot_args[i]) for i in self.plot_valid_keys if i in plot_args])
        self.ax.plot(np.radians(self.x), self.y, **p_args)
        #self.ax.fill_between(np.radians(self.x), 0, self.y, color='0.8')


        # Histogram of colors
        yh, xh = np.histogram(np.radians(self.x), 60, [-np.pi,+np.pi], weights=self.y)
        spacing = xh[1]-xh[0]
        yh = (yh/np.max(yh))*np.max(self.y)

        bins = len(yh)/assumed_symmetry
        color_list = cmap_cyclic_spectrum( np.linspace(0, 1.0, bins, endpoint=True) )
        color_list = np.concatenate( (color_list[bins/2:], color_list[0:bins/2]) ) # Shift
        color_list = np.concatenate( [color_list for i in range(assumed_symmetry)] )


        self.ax.bar(xh[:-1], yh, width=spacing*1.05, color=color_list, linewidth=0.0)


        self.ax.yaxis.set_ticklabels([])
        self.ax.xaxis.set_ticks([np.radians(angle) for angle in range(-180+45, 180+45, +45)])


        self._plot_extra_polar()

        if save:
            if 'dpi' in plot_args:
                plt.savefig(save, dpi=plot_args['dpi'])
            else:
                plt.savefig(save)

        if show:
            self._plot_interact()
            plt.show()

        plt.close(self.fig.number)


    def _plot_extra_polar(self, **plot_args):
        '''This internal function can be over-ridden in order to force additional
        plotting behavior.'''

        pass




    # End class DataLineAngle (DataLine)
    ########################################


# DataHistogram
################################################################################
class DataHistogram(DataLine):

    def _plot_main(self, error=False, error_band=False, dashes=None, **plot_args):

        spacing = self.x[1]-self.x[0]
        plt.bar( self.x, self.y, width=spacing, color='0.8' )

        if error_band:
            l, = plt.plot(self.x, self.y, **plot_args)
            self.ax.fill_between(self.x, self.y-self.y_err, self.y+self.y_err, facecolor='0.8', linewidth=0)

        elif error:
            l = plt.errorbar( self.x, self.y, xerr=self.x_err, yerr=self.y_err, **plot_args)

        if dashes is not None:
            l.set_dashes(dashes)

    # End class DataHistogram(DataLine)
    ########################################



# DataLines
################################################################################
class DataLines(DataLine):
    '''Holds multiple lines, so that they can be plotted together.'''

    def __init__(self, lines=[], plot_args=None, **kwargs):

        self.lines = lines

        self.x_label = kwargs['x_label'] if 'x_label' in kwargs else 'x'
        self.y_label = kwargs['y_label'] if 'y_label' in kwargs else 'y'

        self.x_rlabel = kwargs['x_rlabel'] if 'x_rlabel' in kwargs else self.x_label
        self.y_rlabel = kwargs['y_rlabel'] if 'y_rlabel' in kwargs else self.y_label

        self.plot_valid_keys = ['color', 'linestyle', 'linewidth', 'marker', 'markerfacecolor', 'markersize', 'alpha', 'markeredgewidth', 'markeredgecolor']
        self.plot_args = { 'color' : 'k',
                        'marker' : 'o',
                        'linewidth' : 3.0,
                        'rcParams': {'axes.labelsize': 35,
                                        'xtick.labelsize': 30,
                                        'ytick.labelsize': 30,
                                        },
                            }
        if plot_args: self.plot_args.update(plot_args)



    def add_line(self, line):

        self.lines.append(line)


    def _plot_main(self, error=False, error_band=False, dashes=None, **plot_args):

        for line in self.lines:

            plot_args_current = {}
            plot_args_current.update(self.plot_args)
            plot_args_current.update(plot_args)
            plot_args_current.update(line.plot_args)

            p_args = dict([(i, plot_args_current[i]) for i in self.plot_valid_keys if i in plot_args_current])

            if error_band:
                l, = plt.plot(line.x, line.y, label=line.name, **p_args)
                self.ax.fill_between(line.x, line.y-line.y_err, line.y+line.y_err, facecolor='0.8', linewidth=0)

            elif error:
                l = plt.errorbar( line.x, line.y, xerr=line.x_err, yerr=line.y_err, label=line.name, **p_args)

            else:
                l, = plt.plot(line.x, line.y, label=line.name, **p_args)

            if dashes is not None:
                l.set_dashes(dashes)




    # End class DataLines(object)
    ########################################


# DataLinesStacked
################################################################################
class DataLinesStacked(DataLines):
    '''Holds multiple lines, so that they can be plotted with stacked graphs.'''


    def plot(self, save=None, show=False, plot_range=[None,None,None,None], plot_buffers=[0.25,0.05,0.12,0.05], **kwargs):
        '''Plots the scattering data.

        Parameters
        ----------
        save : str
            Set to 'None' to avoid saving to disk. Provide filename to save.
        show : bool
            Set to true to open an interactive window.
        plot_range : [float, float, float, float]
            Set the range of the plotting (None scales automatically instead).
        '''

        self._plot(save=save, show=show, plot_range=plot_range, plot_buffers=plot_buffers, **kwargs)


    def _plot(self, save=None, show=False, plot_range=[None,None,None,None], plot_buffers=[0.25,0.05,0.12,0.05], error=False, error_band=False, xlog=False, ylog=False, xticks=None, yticks=None, dashes=None, **kwargs):

        num_lines = len(self.lines)

        plot_args = self.plot_args.copy()
        plot_args.update(kwargs)
        self.process_plot_args(**plot_args)

        self.fig = plt.figure( figsize=(10,12), facecolor='white' )
        left_buf, right_buf, bottom_buf, top_buf = plot_buffers
        fig_width = 1.0-right_buf-left_buf
        fig_height = 1.0-top_buf-bottom_buf

        sub_fig_height = fig_height/num_lines

        for i in range(num_lines):

            #self.ax = self.fig.add_axes( [left_buf, bottom_buf, fig_width, fig_height] )
            bottom_pos = bottom_buf + i*sub_fig_height
            setattr(self, 'ax{}'.format(i+1), self.fig.add_axes( [left_buf, bottom_pos, fig_width, sub_fig_height] ) )


        self.ax = getattr(self, 'ax1')


        p_args = dict([(i, plot_args[i]) for i in self.plot_valid_keys if i in plot_args])
        self._plot_main(error=error, error_band=error_band, dashes=dashes, **p_args)


        self.ax.set_xlabel(self.x_rlabel)
        #self.ax.set_ylabel(self.y_rlabel)

        if xlog:
            plt.semilogx()
        if ylog:
            plt.semilogy()
        if xticks is not None:
            self.ax.set_xticks(xticks)
        if yticks is not None:
            self.ax.set_yticks(yticks)


        # Axis scaling
        xi, xf, yi, yf = self.ax.axis()
        if plot_range[0] != None: xi = plot_range[0]
        if plot_range[1] != None: xf = plot_range[1]
        if plot_range[2] != None: yi = plot_range[2]
        if plot_range[3] != None: yf = plot_range[3]
        self.ax.axis( [xi, xf, yi, yf] )

        for i in range(1, num_lines):
            ax = getattr(self, 'ax{}'.format(i+1))
            ax.axis([xi,xf,None,None])
            ax.set_xticklabels([])

            if xticks is not None:
                ax.set_xticks(xticks)


        self._plot_extra(**plot_args)

        if save:
            if 'dpi' in plot_args:
                plt.savefig(save, dpi=plot_args['dpi'])
            else:
                plt.savefig(save)

        if show:
            self._plot_interact()
            plt.show()

        plt.close(self.fig.number)


    def _plot_main(self, error=False, error_band=False, dashes=None, **plot_args):

        for i, line in enumerate(self.lines):

            plot_args_current = {}
            plot_args_current.update(self.plot_args)
            plot_args_current.update(plot_args)
            plot_args_current.update(line.plot_args)

            p_args = dict([(i, plot_args_current[i]) for i in self.plot_valid_keys if i in plot_args_current])

            ax = getattr(self, 'ax{}'.format(i+1))

            if error_band:
                l, = ax.plot(line.x, line.y, **p_args)
                ax.fill_between(line.x, line.y-line.y_err, line.y+line.y_err, facecolor='0.8', linewidth=0)

            elif error:
                l = ax.errorbar( line.x, line.y, xerr=line.x_err, yerr=line.y_err, **p_args)

            else:
                l, = ax.plot(line.x, line.y, **p_args)

            if dashes is not None:
                l.set_dashes(dashes)

            if line.y_rlabel is not None:
                label = line.y_rlabel
            else:
                label = line.y_label
            ax.set_ylabel(label)


    # End class DataLinesStacked(DataLines)
    ########################################




