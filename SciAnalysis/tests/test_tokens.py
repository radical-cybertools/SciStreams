# test the normalizing tokens for the data objects
from dask.base import normalize_token

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
