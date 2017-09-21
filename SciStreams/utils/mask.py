import numpy as np
# mask utilities

def load_master_mask(filename):
    ''' load a master mask format '''
    res = np.load(filename)
    mask =  res['mask']
    refpoint =  res['refpoint']
    rotation_center = res['rotation_center']
    return mask, refpoint, rotation_center
