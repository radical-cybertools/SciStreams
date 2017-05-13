#try a partially filled lattice
import numpy as np
from SciAnalysis.interfaces.StreamDoc import Stream
from dask import compute

# Sample custom stream : fit to a sphere form factor

def SqFitStream(wrapper=None):
    ''' Create a stream for S(q) fit.
        Inputs :
            'sqx'
            'sqy'

        Outputs:
            'sqx'
            'sqy'
            'sqfit'
            'parameters'

    '''
    sin = Stream(wrapper=wrapper)
    s2 = sin.apply(lambda x : x.add_attributes(stream_name="SqFitCustom"))
    s3 = s2.select(('sqx', None), ('sqy', None))
    s4 = s3.map(fitsqsphere)
    sout = s4
    return sin, sout




def fitsqsphere(q, sqdata, Ncutoff=None):
    ''' This will fit to a sphere structure factor.
        The weights are weighted towards high q (q^4)
        output:
            dictionary :
                parameters : the parameters
                sqx : the q values
                sqy : the data
                sqfit : the fit to the data
    '''
    from ScatterSim.NanoObjects import SphereNanoObject, PolydisperseNanoObject
    from lmfit import Model, Parameters

    if Ncutoff is None:
        Ncutoff = 0

    # the function
    def calc_sphereff(q, radius, sigma_R, A, B):
        pargs = {'radius' : radius, 'sigma_R' : sigma_R}
        sphere = PolydisperseNanoObject(SphereNanoObject, pargs=pargs, argname='radius', argstdname='sigma_R')
        sqmodel = sphere.form_factor_squared_isotropic(q)
        return A*sqmodel + B

    # using lmfit
    #radius = 4.41;sigma_R = radius*.08
    params = Parameters()
    params.add('radius', value=4.41)
    params.add('sigma_R', value=0.08)
    params.add('A', value=1e-4, min=0)
    params.add('B', value=1, min=0)

    model = Model(calc_sphereff)
    res = model.fit(sqdata, q=q, params=params, weights=q**4)
    best_fit = res.best_fit

    return dict(parameters=res.best_values, sqx=q, sqy=sqdata, sqfit=best_fit)
