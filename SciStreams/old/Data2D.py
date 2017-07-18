from skbeam.core.utils import radial_grid

''' TODO:
        - add crop, blur and resize


    '''

# All objects must inherit dict
class Data2D(dict):
    def __init__(self, data, name=None, **kwargs):
        '''
            Initialize the data object from a SciResult

            Parameters
            ----------
            data : np.ndarray or SciResult

        '''
        if hasattr(data, get):
            data = data.get(name=name)
        self.name = name


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

    def r_map(self, origin=None):
        '''Returns a map of pixel distances from the origin (measured in pixels).'''
        # from scikit-beam
        if origin==None:
            origin = self.origin
        if pixel_size is None:
            pixel_size = self.pixel_size

        R = radial_grid(origin, self.data.shape, pixel_size=pixel_size)

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

