import matplotlib.pyplot as plt

class Plotter2D:
    ''' 
        The plotter 2D instance. This will plot 2D data.
    '''
    _MAXWIN = 1000
    def __init__(self, data=None, **kwargs):
        self.winnum = int(np.random.uniform()*_MAXWIN)
        if data is not None:
            self.data = data

    def plot(self, winnum=None, clear=True):
        ''' Always clear figure when plotting.'''

        if winnum is None:
            winnum = self.winnum
        plt.figure(winnum);

        if clear:
            plt.clf()

        plt.imshow(self.data)

    def set_data(self, data):
        self.data = data

    def set_bounds(self, data):

    def set_units(self, units):
        self.units = units
