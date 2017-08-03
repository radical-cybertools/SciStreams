# image stitching
import numpy as np


def roundbydigits(n, digits=3):
    ''' round by the number of digits.
        n can be an array

        0, nan or inf are just passed through
    '''
    if n is None:
        return None

    if isinstance(n, np.ndarray):
        w = np.where(~np.isinf(n)*~np.isnan(n)*(n != 0))
        n_new = np.copy(n)
        if len(w[0]) > 0:
            sign = (n[w] > 0)*2 - 1
            power = np.round(np.log10(n[w]*sign), decimals=0).astype(int)
            n_new[w] = np.round(n[w]/10**power, decimals=digits-1)
            n_new[w] = n_new[w]*(10**power)
    else:
        if n == 0 or np.isinf(n) or np.isnan(n):
            n_new = n
        else:
            sign = (n > 0)*2 - 1
            power = np.round(np.log10(n*sign), decimals=0)
            power = int(power)
            n_new = np.round(n/10**power, decimals=digits-1)
            n_new = n_new*(10**power)

    return n_new
