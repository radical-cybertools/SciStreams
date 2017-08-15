import numpy as np
# mask utilities

def load_master_mask(filename):
    ''' load a master mask format '''
    res = np.load(filename)
    mmask =  res['master_mask']
    x0, y0 =  res['x0_master'], res['y0_master']
    return mmask, (y0, x0)
