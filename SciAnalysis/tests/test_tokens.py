# test the normalizing tokens for the data objects
from dask.base import normalize_token
from dask import delayed

def test_calibration():
    ''' Test that the calibration objects are properly tokenized.'''
    from SciAnalysis.analyses.XSAnalysis.Data import Calibration
    from SciAnalysis.analyses.XSAnalysis.DataRQconv import CalibrationRQconv
    cc1 = Calibration(1, 1,1)
    cc2 = CalibrationRQconv(1, 1,1)

    token1 = normalize_token(cc1)
    assert(token1 == ('list', [1, 1, 1]))

    token2 = normalize_token(cc2)
    assert(token2 == (('list', [1, 1, 1]), ('list', [0.0, 0.0, 0.0, 0.0, 0.0, None])))

class newclass:
    def __init__(self, a):
        self.a = a

    def test_print(self):
        print("in function")


from dask import set_options
set_options(delayed_pure=True)

from dask.base import normalize_token
@normalize_token.register(newclass)
def _(obj):
    res = normalize_token(obj.a)
    return res

obj = newclass(1)
dobj = delayed(obj).test_print()
print(dobj)
(dobj.compute())
(dobj.compute())

dobjnew = delayed(obj).test_print()
print(dobjnew)
(dobjnew.compute())
(dobjnew.compute())



