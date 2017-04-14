import matplotlib.pyplot as plt

class Plotter2D:
    def __init__(self, img=None, fignum=None, **kwargs):
        if fignum is None:
            self.fig = plt.figure()
        else:
            self.fig = plt.figure(fignum)

        self.ax = plt.gca()

        if img is not None:
            self.ax.imshow(img)

    def set_extent(self, extent):
        self.extent = extent
        self.ax.cla()
        self.ax.imshow(self.img, extent=self.extent)





