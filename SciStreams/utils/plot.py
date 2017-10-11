import matplotlib.pyplot as plt


def plbox(x0, y0, x1, y1, **kwargs):
    ''' plot box x0, y0 to x1, y1.'''
    plt.plot([x0, x0, x1, x1, x0], [y0, y1, y1, y0, y0], **kwargs)
