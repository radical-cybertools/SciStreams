# Data2D
################################################################################
class Data2D(object):


    def __init__(self, infile=None, format='auto', name=None, **kwargs):

        if name is not None:
            self.name = name
        elif infile is not None:
            self.name = tools.Filename(infile).get_filebase()

        if infile is not None:
            self.load(infile, format=format, **kwargs)

        self.x_label = kwargs['x_label'] if 'x_label' in kwargs else 'x'
        self.y_label = kwargs['y_label'] if 'y_label' in kwargs else 'y'

        self.x_rlabel = kwargs['x_rlabel'] if 'x_rlabel' in kwargs else self.x_label
        self.y_rlabel = kwargs['y_rlabel'] if 'y_rlabel' in kwargs else self.y_label


        self.x_scale = 1.0 # units/pixel
        self.y_scale = 1.0 # units/pixel
        if 'scale' in kwargs:
            self.x_scale = kwargs['scale'] # units/pixel
            self.y_scale = kwargs['scale'] # units/pixel


        self.set_z_display([None, None, 'linear', 1.0])
        self.plot_args = { 'rcParams': {'axes.labelsize': 40,
                                        'xtick.labelsize': 25,
                                        'ytick.labelsize': 25,
                                        },
                            }

        self.origin = [0, 0]

        self.regions = None # Optional overlay highlighting some region of interest

    # Data loading
    ########################################

    def load(self, infile, format='auto', **kwargs):
        '''Loads data from the specified file.'''

        f = tools.Filename(infile)
        ext = f.get_ext()[1:]

        if format=='image' or ext in ['png', 'tif', 'tiff', 'jpg']:
            self.load_image(infile)

        elif format=='npy' or ext=='npy':
            self.load_npy(infile)

        else:
            print("Couldn't identify data format for %s."%(infile))


        self.process_load_args(**kwargs)


    def load_image(self, infile):

        img = PIL.Image.open(infile).convert('I') # 'I' : 32-bit integer pixels
        self.data = np.asarray(img)
        del img


    def load_npy(self, infile, **kwargs):

        self.data = np.load(infile, **kwargs)


    def process_load_args(self, **kwargs):
        '''Follow the directives for the kwargs.'''

        if 'crop_left' in kwargs:
            self.data = self.data[:,kwargs['crop_left']:]
        if 'crop_right' in kwargs:
            self.data = self.data[:,:kwargs['crop_right']]
        if 'crop_top' in kwargs:
            self.data = self.data[kwargs['crop_top']:,:]
        if 'crop_bottom' in kwargs:
            self.data = self.data[:-kwargs['crop_bottom'],:]



    # Coordinate methods
    ########################################

    def get_origin(self):

        return self.origin


    def set_scale(self, scale):
        '''Conversion factor, in "units/pixel" for the image pixels into physical
        dimensions.'''

        # BUG: There is a conflict/inconsistency between the use of origin/scale vs. (x_axis, y_axis).

        self.x_scale = scale
        self.y_scale = scale


    def xy_axes(self):
        # BUG: There is a conflict/inconsistency between the use of origin/scale vs. (x_axis, y_axis).

        dim_y,dim_x = self.data.shape

        if self.origin[0] is None:
            x0 = dim_x/2.
        else:
            x0 = self.origin[0]
        if self.origin[1] is None:
            y0 = dim_y/2.
        else:
            y0 = self.origin[1]

        x_axis = (np.arange(dim_x) - x0)*self.x_scale
        y_axis = (np.arange(dim_y) - y0)*self.y_scale

        return x_axis, y_axis



    def r_map(self, origin=None):
        '''Returns a map of pixel distances from the origin (measured in pixels).'''

        if origin==None:
            origin = self.origin

        dim_y,dim_x = self.data.shape

        if origin[0] is None:
            x0 = dim_x/2.
        else:
            x0 = origin[0]
        if origin[1] is None:
            y0 = dim_y/2.
        else:
            y0 = origin[1]

        x = np.arange(dim_x) - x0
        y = np.arange(dim_y) - y0
        X,Y = np.meshgrid(x,y)
        R = np.sqrt(X**2 + Y**2)

        return R


    def d_map(self, origin=None):
        '''Returns a map of pixel distances from the origin (measured in units).'''

        if origin==None:
            origin = self.origin

        dim_y,dim_x = self.data.shape

        if origin[0] is None:
            x0 = dim_x/2.
        else:
            x0 = origin[0]
        if origin[1] is None:
            y0 = dim_y/2.
        else:
            y0 = origin[1]

        x = (np.arange(dim_x) - x0)*self.x_scale
        y = (np.arange(dim_y) - y0)*self.y_scale
        X,Y = np.meshgrid(x,y)
        R = np.sqrt(X**2 + Y**2)

        return R


    def angle_map(self, origin=None):
        '''Returns a map of the angle for each pixel (w.r.t. origin).
        0 degrees is vertical, +90 degrees is right, -90 degrees is left.'''

        if origin==None:
            origin = self.origin

        dim_y,dim_x = self.data.shape

        if origin[0] is None:
            x0 = dim_x/2.
        else:
            x0 = origin[0]
        if origin[1] is None:
            y0 = dim_y/2.
        else:
            y0 = origin[1]

        x = (np.arange(dim_x) - x0)*self.x_scale
        y = (np.arange(dim_y) - y0)*self.y_scale
        X,Y = np.meshgrid(x,y)
        #M = np.degrees(np.arctan2(Y, X))
        # Note intentional inversion of the usual (x,y) convention.
        # This is so that 0 degrees is vertical.
        M = np.degrees(np.arctan2(X, Y))

        return M



    # Data transformation
    ########################################

    def fft(self):
        '''Return the Fourier Transform of this 2D array (as a Data2D object).'''

        data_fft = Data2DFourier()
        data_fft.data = np.fft.fftn( self.data )

        data_fft.recenter()
        height, width = data_fft.data.shape
        data_fft.origin = [int(width/2), int(height/2)]

        height, width = self.data.shape
        data_fft.x_scale = 2*np.pi/(self.x_scale*width)
        data_fft.y_scale = 2*np.pi/(self.y_scale*height)
        data_fft.x_label = 'qx'
        data_fft.x_rlabel = '$q_x \, (\mathrm{nm}^{-1})$'
        data_fft.y_label = 'qy'
        data_fft.y_rlabel = '$q_y \, (\mathrm{nm}^{-1})$'

        return data_fft



    # Data modification
    ########################################

    def resize(self, zoom, **kwargs):

        #self.data = misc.imresize(self.data, size=1.*zoom, **kwargs)
        self.data = ndimage.interpolation.zoom(self.data, zoom=zoom, **kwargs)

        self.x_scale /= zoom
        self.y_scale /= zoom



    def blur(self, sigma=1.0):
        '''Apply a Gaussian smoothing/blurring to the 2D data. The sigma
        argument specifies the size (in terms of the sigma width of the
        Gaussian, in pixels).'''

        self.data = ndimage.filters.gaussian_filter( self.data, sigma )


    def blur_custom(self, sigma=1.0, accuracy=3.0):
        '''Apply a Gaussian smoothing/blurring to the 2D data. The sigma
        argument specifies the size (in terms of the sigma width of the
        Gaussian, in pixels).

        accuracy is the width of the kernel (3-sigma gives good results,
        anything less truncates the Gaussian).'''

        sigma_x = sigma
        sigma_y = sigma

        filter_size = accuracy*sigma
        filter_box_size = int( 2*filter_size+1 )
        filter_kernel = np.zeros( (filter_box_size, filter_box_size) )

        xc = filter_size
        yc = filter_size

        normalization_x = 1/( np.sqrt(2*np.pi*(sigma_x**2)) )
        normalization_y = 1/( np.sqrt(2*np.pi*(sigma_y**2)) )
        #for ix in range( filter_box_size ):
            #for iy in range( filter_box_size ):
                #filter_kernel[iy, ix] = normalization_x*np.exp( (-( (ix-xc)**2 ))/(2*(sigma_x**2)) )
                #filter_kernel[iy, ix] *= normalization_y*np.exp( (-( (iy-yc)**2 ))/(2*(sigma_y**2)) )


        filter_kernel = np.ones( (filter_box_size, filter_box_size) )
        r = np.asarray([(ix-xc)**2 for ix in range(filter_box_size) ])
        filter_kernel *= normalization_x*np.exp( (-1*( r ))/(2*(sigma_x**2)) )
        r = np.asarray([(iy-yc)**2 for iy in range(filter_box_size) ])
        filter_kernel *= normalization_y*np.exp( (-( r ))/(2*(sigma_y**2)) )[:,np.newaxis]

        self.data = signal.convolve2d(self.data, filter_kernel, mode='same')


    def highpass(self, q, dq):

        data_fft = self.fft()

        distance_map = data_fft.d_map()
        attenuation = 1.0/(1.0 + np.exp( -1.0*(distance_map-q)/dq ) )

        data_fft.data *= attenuation
        data_fft.recenter()

        realspace = np.abs( np.fft.ifftn( data_fft.data ) ) # Inverse FT
        self.data = realspace


    def fourier_filter(self, q, dq):

        data_fft = self.fft()

        distance_map = data_fft.d_map()
        attenuation = np.exp( -1.0*np.square( distance_map-q )/np.square(dq) )

        data_fft.data *= attenuation
        data_fft.recenter()

        realspace = np.real( np.fft.ifftn( data_fft.data ) ) # Inverse FT
        self.data = realspace


    def lowkill(self, dq):

        data_fft = self.fft()

        distance_map = data_fft.d_map()
        attenuation = 1.0 - np.exp( -distance_map/dq )

        data_fft.data *= attenuation
        data_fft.recenter()

        realspace = np.abs( np.fft.ifftn( data_fft.data ) ) # Inverse FT
        self.data = realspace


    def recenter(self):
        '''Shifts the data so that the corners are now combined into the new
        center. The old center is then at the corners.'''

        dim_y, dim_x = self.data.shape
        self.data = np.concatenate( (self.data[dim_y/2:,:], self.data[0:dim_y/2,:]), axis=0 )
        self.data = np.concatenate( (self.data[:,dim_x/2:], self.data[:,0:dim_x/2]), axis=1 )


    def transpose(self):

        self.data = np.transpose(self.data)


    # Data analysis
    ########################################
    def stats(self, prepend='stats_'):

        results = {}

        results[prepend+'max'] = np.max(self.data)
        results[prepend+'min'] = np.min(self.data)
        results[prepend+'average'] = np.average(self.data)
        results[prepend+'std'] = np.std(self.data)
        results[prepend+'N'] = len(self.data.ravel())
        results[prepend+'total'] = np.sum(self.data.ravel())

        results[prepend+'skew'] = stats.skew(self.data.ravel())

        results[prepend+'spread'] = results[prepend+'max'] - results[prepend+'min']
        results[prepend+'std_rel'] = results[prepend+'std'] / results[prepend+'average']

        return results



    # Data reduction
    ########################################
    def circular_average(self, abs=False, x_label='r', x_rlabel='$r$', y_label='I', y_rlabel=r'$\langle I \rangle \, (\mathrm{counts/pixel})$', **kwargs):
        '''Returns a 1D curve that is a circular average of the 2D data.'''

        mask = np.ones(self.data.shape)
        dim_y, dim_x = self.data.shape
        x0, y0 = self.origin

        # .ravel() is used to convert the 2D grids into 1D arrays.
        # This is not strictly necessary, but improves speed somewhat.

        data = self.data.ravel()
        if abs:
            data = np.abs(data)
        pixel_list = np.where(mask.ravel()==1) # Non-masked pixels

        # Generate map of distances-from-origin
        R = self.d_map().ravel()
        scale = (self.x_scale + self.y_scale)/2.0
        Rd = (R/scale + 0.5).astype(int) # Simplify the R pixel-distances to closest integers

        num_per_R = np.bincount(Rd[pixel_list])
        idx = np.where(num_per_R!=0) # R-distances that actually have data

        r_vals = np.bincount( Rd[pixel_list], weights=R[pixel_list] )[idx]/num_per_R[idx]
        I_vals = np.bincount( Rd[pixel_list], weights=data[pixel_list] )[idx]/num_per_R[idx]

        line = DataLine( x=r_vals, y=I_vals, x_label=x_label, y_label=y_label, x_rlabel=x_rlabel, y_rlabel=y_rlabel )

        return line


    # Data extraction
    ########################################
    def linecut_angle(self, d_center, d_spread, abs=False, x_label='angle', x_rlabel='$\chi \, (^{\circ})$', y_label='I', y_rlabel=r'$I (\chi) \, (\mathrm{counts/pixel})$', **kwargs):


        mask = np.ones(self.data.shape)
        dim_y, dim_x = self.data.shape
        x0, y0 = self.origin

        # .ravel() is used to convert the 2D grids into 1D arrays.
        # This is not strictly necessary, but improves speed somewhat.

        data = self.data.ravel()
        if abs:
            data = np.abs(data)
        pixel_list = np.where(mask.ravel()==1) # Non-masked pixels

        # Generate map
        M = self.angle_map().ravel()
        scale_x = np.abs(np.arctan(1.0/(d_center/self.x_scale)))
        scale_y = np.abs(np.arctan(1.0/(d_center/self.y_scale)))
        scale = np.degrees( min(scale_x, scale_y) ) # approximately 1-pixel

        Md = (M/scale + 0.5).astype(int) # Simplify the distances to closest integers
        Md -= np.min(Md)

        num_per_m = np.bincount(Md[pixel_list])
        idx = np.where(num_per_m!=0) # distances that actually have data

        x_vals = np.bincount( Md[pixel_list], weights=M[pixel_list] )[idx]/num_per_m[idx]
        I_vals = np.bincount( Md[pixel_list], weights=data[pixel_list] )[idx]/num_per_m[idx]

        line = DataLineAngle( x=x_vals, y=I_vals, x_label=x_label, y_label=y_label, x_rlabel=x_rlabel, y_rlabel=y_rlabel )

        return line


    # Plotting
    ########################################

    def set_z_display(self, z_display):
        '''Controls how the z-values are converted into the false colormap.
        The provided array should have 4 elements. Example:
        [ 0, 10, 'gamma', 0.3]
         min max  mode    adjustment

        If min or max is set to 'None', then ztrim is used to pick values.
        mode can be:
          'linear'             adj ignored
          'log'                adj ignored
          'gamma'              adj is the log_gamma value
          'r'                  adj is the exponent

        'gamma' is a log-like gamma-correction function. 'adjustment' is the log_gamma value.
            log_gamma of 0.2 to 0.5 gives a nice 'logarithmic' response
            large values of log_gamma give a progressively more nearly response
            log_gamma = 2.0 gives a nearly linear response
            log_gamma < 0.2 give a very sharp response

        'r' multiplies the data by r**(adj), which can help to normalize data
        that decays away from a central origin.

        '''

        self.z_display = z_display


    def plot_image(self, save, ztrim=[0.01, 0.01], **kwargs):
        '''Generates a false-color image of the 2D data.'''

        if 'cmap' in kwargs:
            cmap = kwargs['cmap']

        else:
            # http://matplotlib.org/examples/color/colormaps_reference.html
            #cmap = mpl.cm.RdBu
            #cmap = mpl.cm.RdBu_r
            #cmap = mpl.cm.hot
            #cmap = mpl.cm.gist_heat
            #cmap = mpl.cm.gist_earth
            cmap = mpl.cm.jet

        #img = Image.open(filename).convert("I")
        #img = Image.open(infile).convert("L") # black-and-white


        values = np.sort( self.data.flatten() )
        if 'zmin' in kwargs and kwargs['zmin'] is not None:
            zmin = kwargs['zmin']
        elif self.z_display[0] is not None:
            zmin = self.z_display[0]
        else:
            zmin = values[ +int( len(values)*ztrim[0] ) ]

        if 'zmax' in kwargs and kwargs['zmax'] is not None:
            zmax = kwargs['zmax']
        elif self.z_display[1] is not None:
            zmax = self.z_display[1]
        else:
            idx = -int( len(values)*ztrim[1] )
            if idx>=0:
                idx = -1
            zmax = values[idx]

        if zmax==zmin:
            zmax = max(values)

        self.z_display[0] = zmin
        self.z_display[1] = zmax
        self._plot_z_transform()

        img = PIL.Image.fromarray(np.uint8(cmap(self.Z)*255))

        img.save(save)


    def plot(self, save=None, show=False, ztrim=[0.01, 0.01], size=10.0, plot_buffers=[0.15,0.05,0.15,0.05], **kwargs):
        '''Plots the data.

        Parameters
        ----------
        save : str
            Set to 'None' to avoid saving to disk. Provide filename to save.
        show : bool
            Set to true to open an interactive window.
        ztrim : [float, float]
            Specify how to auto-set the z-scale. The floats indicate how much of
            the z-scale to 'trim' (relative units; i.e. 0.05 indicates 5%).
        '''

        self._plot(save=save, show=show, ztrim=ztrim, size=size, plot_buffers=plot_buffers, **kwargs)


    def _plot(self, save=None, show=False, ztrim=[0.01, 0.01], size=10.0, plot_buffers=[0.1,0.1,0.1,0.1], **kwargs):

        plot_args = self.plot_args.copy()
        plot_args.update(kwargs)
        self.process_plot_args(**plot_args)


        self.fig = plt.figure( figsize=(size,size), facecolor='white' )
        left_buf, right_buf, bottom_buf, top_buf = plot_buffers
        fig_width = 1.0-right_buf-left_buf
        fig_height = 1.0-top_buf-bottom_buf
        self.ax = self.fig.add_axes( [left_buf, bottom_buf, fig_width, fig_height] )



        # Set zmin and zmax. Top priority is given to a kwarg to this plot function.
        # If that is not set, the value set for this object is used. If neither are
        # specified, a value is auto-selected using ztrim.

        values = np.sort( self.data.flatten() )
        if 'zmin' in plot_args and plot_args['zmin'] is not None:
            zmin = plot_args['zmin']
        elif self.z_display[0] is not None:
            zmin = self.z_display[0]
        else:
            zmin = values[ +int( len(values)*ztrim[0] ) ]

        if 'zmax' in plot_args and plot_args['zmax'] is not None:
            zmax = plot_args['zmax']
        elif self.z_display[1] is not None:
            zmax = self.z_display[1]
        else:
            idx = -int( len(values)*ztrim[1] )
            if idx>=0:
                idx = -1
            zmax = values[idx]

        if zmax==zmin:
            zmax = max(values)

        print( '        data: %.1f to %.1f\n        z-scaling: %.1f to %.1f\n' % (np.min(self.data), np.max(self.data), zmin, zmax) )

        self.z_display[0] = zmin
        self.z_display[1] = zmax
        self._plot_z_transform()


        shading = 'flat'
        #shading = 'gouraud'

        if 'cmap' in plot_args:
            cmap = plot_args['cmap']

        else:
            # http://matplotlib.org/examples/color/colormaps_reference.html
            #cmap = mpl.cm.RdBu
            #cmap = mpl.cm.RdBu_r
            #cmap = mpl.cm.hot
            #cmap = mpl.cm.gist_heat
            cmap = mpl.cm.jet

        x_axis, y_axis = self.xy_axes()
        extent = [x_axis[0], x_axis[-1], y_axis[0], y_axis[-1]]

        # TODO: Handle 'origin' correctly. (E.g. allow it to be set externally.)
        self.im = plt.imshow(self.Z, vmin=0, vmax=1, cmap=cmap, interpolation='nearest', extent=extent, origin='lower')
        #plt.pcolormesh( self.x_axis, self.y_axis, self.Z, cmap=cmap, vmin=zmin, vmax=zmax, shading=shading )

        if self.regions is not None:
            for region in self.regions:
                plt.imshow(region, cmap=mpl.cm.spring, interpolation='nearest', alpha=0.75)
                #plt.imshow(np.flipud(region), cmap=mpl.cm.spring, interpolation='nearest', alpha=0.75, origin='lower')

        x_label = self.x_rlabel if self.x_rlabel is not None else self.x_label
        y_label = self.y_rlabel if self.y_rlabel is not None else self.y_label
        plt.xlabel(x_label)
        plt.ylabel(y_label)

        if 'xticks' in kwargs and kwargs['xticks'] is not None:
            self.ax.set_xticks(kwargs['xticks'])
        if 'yticks' in kwargs and kwargs['yticks'] is not None:
            self.ax.set_yticks(kwargs['yticks'])


        if 'plot_range' in plot_args:
            plot_range = plot_args['plot_range']
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


    def _plot_extra(self, **plot_args):
        '''This internal function can be over-ridden in order to force additional
        plotting behavior.'''

        pass


    def _plot_z_transform(self):
        '''Rescales the data according to the internal z_display setting.'''

        zmin, zmax, zmode, zadj = self.z_display

        if zmode=='log':
            #Z = np.log( (self.data-zmin)/(zmax-zmin) )

            #Z = np.log(self.data)/np.log(zmax)

            zmin = max(zmin,0.5)
            Z = (np.log(self.data)-np.log(zmin))/(np.log(zmax)-np.log(zmin))

        elif zmode=='gamma':
            log_gamma = zadj
            c = np.exp(1/log_gamma) - 1
            Z = (self.data-zmin)/(zmax-zmin)
            Z = log_gamma*np.log(Z*c + 1)

        elif zmode=='r':
            Z = self.data*np.power( self.r_map(), zadj )
            Z = (Z-zmin)/(zmax-zmin)

        elif zmode=='linear':
            Z = (self.data-zmin)/(zmax-zmin)

        else:
            print('Warning: z_display mode %s not recognized.'%(zmode))
            Z = (self.data-zmin)/(zmax-zmin)

        self.Z = np.nan_to_num(Z)


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
        self.fig.canvas.mpl_connect('key_press_event', self._key_press_event)

        self.ax.format_coord = self._format_coord


    def _format_coord_simple(self, x, y):

        col = int(x+0.5)
        row = int(y+0.5)

        numrows, numcols = self.data.shape
        row = numrows-row-1
        if col>=0 and col<numcols and row>=0 and row<numrows:
            z = self.data[row,col]
            #z = self.Z[row,col]
            #return 'x=%1.1f, y=%1.1f, z=%1.1f'%(x, y, z)
            #return 'r%dc%d x=%1.1f, y=%1.1f, z=%g'%(row, col, x, y, z)
            return 'x=%1.1f, y=%1.1f, z=%g'%(x, y, z)
        else:
            return 'x=%1.1f, y=%1.1f'%(x, y)


    def _format_coord(self, x, y):

        xp = self.origin[0] + x/self.x_scale
        yp = self.origin[1] + y/self.y_scale

        col = int(xp+0.5)
        row = int(yp+0.5)

        numrows, numcols = self.data.shape
        row = numrows-row-1
        if col>=0 and col<numcols and row>=0 and row<numrows:
            z = self.data[row,col]
            #z = self.Z[row,col]
            #return 'x=%1.1f, y=%1.1f, z=%1.1f'%(x, y, z)
            return 'x=%g, y=%g, z=%g'%(x, y, z)
        else:
            return 'x=%g, y=%g'%(x, y)


    def _key_press_event(self, event):
        '''Gets called when a key is pressed when the plot is open.'''

        update = False

        if event.key == '[':
            self.z_display[3] *= 1.0/1.5
            update = True

        elif event.key == ']':
            self.z_display[3] *= 1.5
            update = True

        elif event.key == '-' or event.key=='_':
            self.z_display[1] *= 1.0/4.0
            update = True

        elif event.key == '+' or event.key=='=':
            self.z_display[1] *= 4.0
            update = True

        elif event.key == 'o':
            self.z_display[0] *= 1.0/4.0
            update = True

        elif event.key == 'p':
            if self.z_display[0]==0:
                self.z_display[0] = 1
            self.z_display[0] *= 4.0
            update = True

        elif event.key == 'm':
            if self.z_display[2]=='gamma':
                self.z_display[2] = 'linear'
            else:
                self.z_display[2] = 'gamma'
            update = True


        if update:
            #print( self.z_display)
            print('            zmin: %.1f, zmax: %.1f, %s (%.2f)'%(self.z_display[0], self.z_display[1], str(self.z_display[2]), self.z_display[3]))

            self._plot_z_transform()
            self.im.set_data(self.Z)
            self.fig.canvas.draw()


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


    # End class Data2D(object)
    ########################################




# Data2DFT
################################################################################
class Data2DFourier(Data2D):

    def __init__(self, infile=None, format='auto', name=None, **kwargs):
        '''Creates a new Data2D object, which stores Fourier data.'''

        super(Data2DFourier, self).__init__(infile=None, format=format, name=name, **kwargs)

        # TODO: Default axis names, etc.



    def plot(self, save=None, show=False, ztrim=[0.05, 0.001], size=10.0, plot_buffers=[0.18,0.04,0.18,0.04], blur=None, **kwargs):
        '''Plots the scattering data.

        Parameters
        ----------
        save : str
            Set to 'None' to avoid saving to disk. Provide filename to save.
        show : bool
            Set to true to open an interactive window.
        ztrim : [float, float]
            Specify how to auto-set the z-scale. The floats indicate how much of
            the z-scale to 'trim' (relative units; i.e. 0.05 indicates 5%).
        '''

        Fourier_data = self.data
        self.data = np.abs(self.data)
        if blur is not None:
            self.blur(blur)
        self._plot(save=save, show=show, ztrim=ztrim, size=size, plot_buffers=plot_buffers, **kwargs)
        self.data = Fourier_data


    def plot_components(self, save=None, show=False, ztrim=[0.05, 0.001], size=10.0, plot_buffers=[0.18,0.04,0.18,0.04], blur=None, **kwargs):

        Fourier_data = self.data

        # Absolute value
        save_current = tools.Filename(save)
        save_current.append('-Abs')
        save_current = save_current.get_filepath()
        self.data = np.abs(Fourier_data)
        if blur is not None:
            self.blur(blur)
        self._plot(save=save_current, show=show, ztrim=ztrim, size=size, plot_buffers=plot_buffers, **kwargs)
        self.z_display[0] = None
        self.z_display[1] = None


        # Real part
        save_current = tools.Filename(save)
        save_current.append('-Re')
        save_current = save_current.get_filepath()
        self.data = np.real(Fourier_data)
        if blur is not None:
            self.blur(blur)
        #zmax = np.max(np.abs(self.data))*(0.05)
        self._plot(save=save_current, show=show, ztrim=ztrim, size=size, plot_buffers=plot_buffers, cmap='gnuplot2', **kwargs)
        self.z_display[0] = None
        self.z_display[1] = None

        # Imaginary part
        save_current = tools.Filename(save)
        save_current.append('-Im')
        save_current = save_current.get_filepath()
        self.data = np.imag(Fourier_data)
        if blur is not None:
            self.blur(blur)
        self._plot(save=save_current, show=show, ztrim=[ztrim[1],ztrim[1]], size=size, plot_buffers=plot_buffers, cmap='seismic', **kwargs)
        self.z_display[0] = None
        self.z_display[1] = None

        # Phase part
        save_current = tools.Filename(save)
        save_current.append('-Phase')
        save_current = save_current.get_filepath()
        self.data = np.angle(Fourier_data)
        #self.data = np.radians( self.angle_map() ) # For testing
        if blur is not None:
            self.blur(blur)
        cmap = 'hsv'
        cmap = mpl.colors.LinearSegmentedColormap.from_list('cmap_current', ['red', 'blue', 'red'])
        self._plot(save=save_current, show=show, zmin=-np.pi, zmax=+np.pi, cmap=cmap, size=size, plot_buffers=plot_buffers, **kwargs)
        self.z_display[0] = None
        self.z_display[1] = None


        # Set data back to normal
        self.data = Fourier_data


    # Data reduction
    ########################################

    def circular_average(self, abs=True, x_label='q', x_rlabel='$q$', y_label='I', y_rlabel=r'$\langle I \rangle \, (\mathrm{counts/pixel})$', **kwargs):
        '''Returns a 1D curve that is a circular average of the 2D data.'''

        return super(Data2DFourier, self).circular_average(abs=abs, x_label=x_label, x_rlabel=x_rlabel, y_label=y_label, y_rlabel=y_rlabel, **kwargs)



    # Data remeshing
    ########################################

    def remesh_q_phi(self, bins_relative=1.0, bins_phi=None, **kwargs):
        '''Converts the data from Fourier-space into a (q,phi) map.'''

        # Determine limits
        dq = (self.x_scale + self.y_scale)*0.5

        Q = self.d_map().ravel()
        q_min = np.min(np.abs(Q))
        q_max = np.max(np.abs(Q))
        q_mid = q_max-q_min

        if bins_phi is None:
            dphi = np.degrees(np.arctan(dq/q_mid))
            bins_phi = 360.0/dphi

        else:
            dphi = 360.0/bins_phi

        PHI = self.angle_map().ravel() # degrees
        #phi_min = np.min(PHI)
        #phi_max = np.max(PHI)
        phi_min = -180.0
        phi_max = +180.0

        D = self.data.ravel()



        bins = [ int( abs(phi_max-phi_min)/dphi ) , int( abs(q_max-q_min)/dq ) ]

        remesh_data, zbins, xbins = np.histogram2d(PHI, Q, bins=bins, range=[[phi_min,phi_max], [q_min,q_max]], normed=False, weights=D)
        #num_per_bin, zbins, xbins = np.histogram2d(QZ, QX, bins=bins, range=[[qz_min,qz_max], [qx_min,qx_max]], normed=False, weights=None)
        #remesh_data = np.nan_to_num( remesh_data/num_per_bin )



        class Data2DQPhi(Data2D):
            '''Represents a 2D (q,phi) map.
            '''
            def __init__(self, infile=None, format='auto', name=None, **kwargs):
                iargs = {
                        'x_label' : 'q',
                        'x_rlabel' : '$q \, (\AA^{-1})$',
                        'y_label' : 'phi',
                        'y_rlabel' : '$\phi \, (^{\circ})$',
                        }
                iargs.update(kwargs)

                super(Data2DQPhi, self).__init__(infile=None, format=format, name=name, **iargs)

                self.plot_args = { 'rcParams': {'axes.labelsize': 48,
                                                'xtick.labelsize': 33,
                                                'ytick.labelsize': 33,
                                                },
                                    }
            def xy_axes(self):
                return self.x_axis, self.y_axis

            def _plot_extra(self, **plot_args):
                self.ax.set_aspect('auto')



        q_phi_data = Data2DQPhi()
        q_phi_data.data = remesh_data
        q_phi_data.x_axis = xbins[:-1] + (xbins[1]-xbins[0]) # convert from bin edges to bin centers
        q_phi_data.y_axis = zbins[:-1] + (zbins[1]-zbins[0]) # convert from bin edges to bin centers

        q_phi_data.x_scale = xbins[1]-xbins[0]
        q_phi_data.y_scale = zbins[1]-zbins[0]

        q_phi_data.x_label = 'q'
        q_phi_data.x_rlabel = '$q \, (\AA^{-1})$'
        q_phi_data.y_label = 'phi'
        q_phi_data.y_rlabel = '$\phi \, (^{\circ})$'



        return q_phi_data



    # End class Data2DFT(object)
    ########################################

